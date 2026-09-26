"""SkyGuard Phase 3H device registry and revocation service.

Prototype scope:
- One independent identity per simulated AWS device.
- Device <-> station binding.
- ACTIVE / REVOKED lifecycle.
- Key fingerprint (never stores the secret itself).
- Registry audit records use the existing edge_security_audit table when available.

Production target remains hardware-backed per-device secrets (for example, ESP32
secure facilities + HKDF), as documented in the SkyGuard specification.
"""
from __future__ import annotations

from db_utils import db_connect

import hashlib
import sqlite3
from datetime import datetime
from typing import Any

from edge_security import derive_device_key, get_master_secret
from config import STATIONS, canonical_station_name

REGISTRY_TABLE = "device_registry"


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def key_fingerprint(device_id: str) -> str:
    return hashlib.sha256(derive_device_key(device_id, get_master_secret())).hexdigest()[:16]


def ensure_registry_table(db_path: str) -> None:
    with db_connect(db_path, timeout=10) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS device_registry (
                device_id TEXT PRIMARY KEY,
                station_id TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                provisioning_source TEXT NOT NULL DEFAULT 'MANUAL',
                key_fingerprint TEXT NOT NULL,
                provisioned_at TEXT NOT NULL,
                revoked_at TEXT,
                restored_at TEXT,
                last_seen TEXT,
                last_sequence INTEGER,
                reject_count INTEGER NOT NULL DEFAULT 0
            )"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_device_registry_station ON device_registry(station_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_device_registry_status ON device_registry(status)")
        conn.commit()


def registry_count(db_path: str) -> int:
    with db_connect(db_path, timeout=10) as conn:
        return int(conn.execute("SELECT COUNT(*) FROM device_registry").fetchone()[0] or 0)


def provision(db_path: str, device_id: str, station_id: str, source: str = "MANUAL", allow_revoked: bool = False) -> dict[str, Any]:
    device_id = str(device_id or "").strip()
    station_id = canonical_station_name(station_id or "")
    if not device_id:
        raise ValueError("Missing device_id")
    if station_id not in STATIONS:
        raise ValueError(f"Unknown station_id: {station_id}")
    ensure_registry_table(db_path)
    now = now_text()
    fp = key_fingerprint(device_id)
    with db_connect(db_path, timeout=10) as conn:
        row = conn.execute(
            "SELECT device_id,station_id,status,provisioning_source,key_fingerprint,provisioned_at,revoked_at,restored_at,last_seen,last_sequence,reject_count FROM device_registry WHERE device_id=?",
            (device_id,),
        ).fetchone()
        station_owner = conn.execute("SELECT device_id,status FROM device_registry WHERE station_id=?", (station_id,)).fetchone()
        if station_owner and station_owner[0] != device_id and station_owner[1] != "REVOKED":
            raise ValueError(f"Station {station_id} is already bound to another active device")
        if row:
            status = row[2]
            if status == "ACTIVE":
                return _row_to_dict(row)
            if status == "REVOKED" and not allow_revoked:
                raise PermissionError(f"Device {device_id} is revoked; restore it explicitly before use")
            conn.execute(
                "UPDATE device_registry SET station_id=?, status='ACTIVE', restored_at=?, revoked_at=NULL, provisioning_source=?, key_fingerprint=? WHERE device_id=?",
                (station_id, now, source, fp, device_id),
            )
            conn.commit()
            _audit(conn, device_id, station_id, "PROVISIONED", f"Device restored/provisioned by {source}")
            conn.commit()
        else:
            conn.execute(
                "INSERT INTO device_registry(device_id,station_id,status,provisioning_source,key_fingerprint,provisioned_at) VALUES(?,?,?,?,?,?)",
                (device_id, station_id, "ACTIVE", source, fp, now),
            )
            _audit(conn, device_id, station_id, "PROVISIONED", f"Device provisioned by {source}")
            conn.commit()
        return get_device(db_path, device_id) or {}




def provision_batch(db_path: str, devices: list[dict[str, Any]], source: str = "BATCH_API", allow_revoked: bool = False) -> list[dict[str, Any]]:
    """Provision/validate multiple devices in one SQLite transaction.

    This avoids ten sequential HTTP+SQLite writer transactions during live
    simulator startup. Existing ACTIVE devices are treated as idempotent.
    """
    if not isinstance(devices, list) or not devices:
        raise ValueError("devices must be a non-empty list")
    ensure_registry_table(db_path)
    now = now_text()
    normalized=[]
    seen_devices=set(); seen_stations=set()
    for item in devices:
        if not isinstance(item, dict):
            raise ValueError("Each device entry must be an object")
        device_id=str(item.get("device_id") or "").strip()
        station_id=canonical_station_name(item.get("station_id") or item.get("station_name") or "")
        if not device_id:
            raise ValueError("Missing device_id")
        if station_id not in STATIONS:
            raise ValueError(f"Unknown station_id: {station_id}")
        if device_id in seen_devices:
            raise ValueError(f"Duplicate device_id in batch: {device_id}")
        if station_id in seen_stations:
            raise ValueError(f"Duplicate station_id in batch: {station_id}")
        seen_devices.add(device_id); seen_stations.add(station_id)
        normalized.append((device_id, station_id, str(item.get("source") or source)))
    result=[]
    with db_connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("BEGIN IMMEDIATE")
        for device_id, station_id, item_source in normalized:
            fp=key_fingerprint(device_id)
            row=conn.execute("SELECT device_id,station_id,status,provisioning_source,key_fingerprint,provisioned_at,revoked_at,restored_at,last_seen,last_sequence,reject_count FROM device_registry WHERE device_id=?", (device_id,)).fetchone()
            station_owner=conn.execute("SELECT device_id,status FROM device_registry WHERE station_id=?", (station_id,)).fetchone()
            if station_owner and station_owner[0] != device_id and station_owner[1] != "REVOKED":
                raise ValueError(f"Station {station_id} is already bound to another active device")
            if row:
                if row[2] == "ACTIVE":
                    result.append(_row_to_dict(row))
                    continue
                if row[2] == "REVOKED" and not allow_revoked:
                    raise PermissionError(f"Device {device_id} is revoked; restore it explicitly before use")
                conn.execute("UPDATE device_registry SET station_id=?, status='ACTIVE', restored_at=?, revoked_at=NULL, provisioning_source=?, key_fingerprint=? WHERE device_id=?", (station_id,now,item_source,fp,device_id))
                _audit(conn,device_id,station_id,"PROVISIONED",f"Device restored/provisioned by {item_source}")
            else:
                conn.execute("INSERT INTO device_registry(device_id,station_id,status,provisioning_source,key_fingerprint,provisioned_at) VALUES(?,?,?,?,?,?)", (device_id,station_id,"ACTIVE",item_source,fp,now))
                _audit(conn,device_id,station_id,"PROVISIONED",f"Device provisioned by {item_source}")
            row2=conn.execute("SELECT device_id,station_id,status,provisioning_source,key_fingerprint,provisioned_at,revoked_at,restored_at,last_seen,last_sequence,reject_count FROM device_registry WHERE device_id=?", (device_id,)).fetchone()
            result.append(_row_to_dict(row2))
        conn.commit()
    return result


def restore(db_path: str, device_id: str, source: str = "MANUAL") -> dict[str, Any]:
    device_id = str(device_id or "").strip()
    ensure_registry_table(db_path)
    with db_connect(db_path, timeout=10) as conn:
        row = conn.execute("SELECT station_id,status FROM device_registry WHERE device_id=?", (device_id,)).fetchone()
        if not row:
            raise KeyError(f"Unknown device: {device_id}")
        if row[1] == "ACTIVE":
            return get_device(db_path, device_id) or {}
        now = now_text()
        conn.execute("UPDATE device_registry SET status='ACTIVE', restored_at=?, revoked_at=NULL WHERE device_id=?", (now, device_id))
        _audit(conn, device_id, row[0], "RESTORED", f"Device restored by {source}")
        conn.commit()
    return get_device(db_path, device_id) or {}


def revoke(db_path: str, device_id: str, source: str = "MANUAL", reason: str = "") -> dict[str, Any]:
    device_id = str(device_id or "").strip()
    ensure_registry_table(db_path)
    with db_connect(db_path, timeout=10) as conn:
        row = conn.execute("SELECT station_id,status FROM device_registry WHERE device_id=?", (device_id,)).fetchone()
        if not row:
            raise KeyError(f"Unknown device: {device_id}")
        now = now_text()
        conn.execute("UPDATE device_registry SET status='REVOKED', revoked_at=? WHERE device_id=?", (now, device_id))
        _audit(conn, device_id, row[0], "REVOKED", reason or f"Device revoked by {source}")
        conn.commit()
    return get_device(db_path, device_id) or {}


def authorize(db_path: str, device_id: str, station_id: str, sequence: int | None = None) -> dict[str, Any]:
    device_id = str(device_id or "").strip()
    station_id = canonical_station_name(station_id or "")
    ensure_registry_table(db_path)
    with db_connect(db_path, timeout=10) as conn:
        total = int(conn.execute("SELECT COUNT(*) FROM device_registry").fetchone()[0] or 0)
        row = conn.execute("SELECT device_id,station_id,status,last_sequence,reject_count FROM device_registry WHERE device_id=?", (device_id,)).fetchone()
        # Migration compatibility: an entirely empty registry does not block old
        # Phase 2D secure test clients. As soon as a device is provisioned, the
        # registry becomes authoritative and unknown devices are rejected.
        if total == 0:
            return {"authorized": True, "mode": "LEGACY_EMPTY_REGISTRY", "device_id": device_id, "station_id": station_id}
        if not row:
            return {"authorized": False, "reason": "Unknown device", "device_id": device_id, "station_id": station_id}
        if canonical_station_name(row[1]) != station_id:
            return {"authorized": False, "reason": "Device/station binding mismatch", "device_id": device_id, "station_id": station_id}
        if row[2] != "ACTIVE":
            return {"authorized": False, "reason": f"Device is {row[2]}", "device_id": device_id, "station_id": station_id}
        if sequence is not None:
            try:
                sequence = int(sequence)
            except (TypeError, ValueError):
                return {"authorized": False, "reason": "Invalid sequence", "device_id": device_id, "station_id": station_id}
            if sequence <= 0:
                return {"authorized": False, "reason": "Invalid sequence: must be positive", "device_id": device_id, "station_id": station_id}
            last_sequence = row[3]
            if last_sequence is not None and sequence <= int(last_sequence):
                return {
                    "authorized": False,
                    "reason": f"Replay or stale sequence: {sequence} <= last accepted {int(last_sequence)}",
                    "device_id": device_id,
                    "station_id": station_id,
                    "last_sequence": int(last_sequence),
                }
        now = now_text()
        conn.execute("UPDATE device_registry SET last_seen=?, last_sequence=COALESCE(?,last_sequence) WHERE device_id=?", (now, sequence, device_id))
        conn.commit()
        return {"authorized": True, "mode": "REGISTRY", "device_id": device_id, "station_id": station_id, "last_sequence": row[3]}


def record_rejection(db_path: str, device_id: str) -> None:
    ensure_registry_table(db_path)
    with db_connect(db_path, timeout=10) as conn:
        conn.execute("UPDATE device_registry SET reject_count=reject_count+1 WHERE device_id=?", (device_id,))
        conn.commit()


def get_device(db_path: str, device_id: str) -> dict[str, Any] | None:
    with db_connect(db_path, timeout=10) as conn:
        row = conn.execute(
            "SELECT device_id,station_id,status,provisioning_source,key_fingerprint,provisioned_at,revoked_at,restored_at,last_seen,last_sequence,reject_count FROM device_registry WHERE device_id=?",
            (device_id,),
        ).fetchone()
    return _row_to_dict(row) if row else None


def list_registry(db_path: str) -> list[dict[str, Any]]:
    ensure_registry_table(db_path)
    with db_connect(db_path, timeout=10) as conn:
        rows = conn.execute(
            "SELECT device_id,station_id,status,provisioning_source,key_fingerprint,provisioned_at,revoked_at,restored_at,last_seen,last_sequence,reject_count FROM device_registry ORDER BY station_id"
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def summary(db_path: str) -> dict[str, int]:
    ensure_registry_table(db_path)
    with db_connect(db_path, timeout=10) as conn:
        total, active, revoked = conn.execute(
            "SELECT COUNT(*), SUM(CASE WHEN status='ACTIVE' THEN 1 ELSE 0 END), SUM(CASE WHEN status='REVOKED' THEN 1 ELSE 0 END) FROM device_registry"
        ).fetchone()
    return {"total": int(total or 0), "active": int(active or 0), "revoked": int(revoked or 0)}


def recent_audit(db_path: str, limit: int = 25) -> list[dict[str, Any]]:
    with db_connect(db_path, timeout=10) as conn:
        rows = conn.execute(
            "SELECT id,device_id,station_name,event_type,detail,created_at FROM edge_security_audit ORDER BY id DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    return [{"id": int(r[0]), "device_id": r[1], "station_id": r[2], "event_type": r[3], "detail": r[4], "created_at": r[5]} for r in rows]


def _audit(conn: sqlite3.Connection, device_id: str, station_id: str, event_type: str, detail: str) -> None:
    # edge_security_audit is created by app startup; create it here as well so
    # direct module usage remains safe.
    conn.execute(
        """CREATE TABLE IF NOT EXISTS edge_security_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT, device_id TEXT, station_name TEXT, event_type TEXT NOT NULL,
            detail TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""
    )
    conn.execute("INSERT INTO edge_security_audit(device_id,station_name,event_type,detail) VALUES(?,?,?,?)", (device_id, station_id, event_type, detail[:500]))


def _row_to_dict(row: tuple[Any, ...] | None) -> dict[str, Any]:
    if not row:
        return {}
    return {
        "device_id": row[0], "station_id": row[1], "status": row[2], "provisioning_source": row[3],
        "key_fingerprint": row[4], "provisioned_at": row[5], "revoked_at": row[6], "restored_at": row[7],
        "last_seen": row[8], "last_sequence": row[9], "reject_count": int(row[10] or 0),
    }
