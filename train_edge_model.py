"""Reproducible offline training/export for the SkyGuard Edge AI gate.

The exported JSON is a tiny portable decision tree intended for microcontroller
inference. Runtime inference in edge_ai.py does NOT require scikit-learn.
"""
from __future__ import annotations

from pathlib import Path
import json
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier

CSV = Path(__file__).resolve().with_name("10_AWS_stations_combined.csv")
OUT = Path(__file__).resolve().with_name("edge_ai_model.json")
RNG = np.random.default_rng(2026)


def features_for_row(r, hist):
    last = hist.iloc[-1] if len(hist) else r
    return {
        "temperature": float(r.temperature),
        "pressure": float(r.pressure),
        "humidity": float(r.humidity),
        "abs_temp_delta": abs(float(r.temperature) - float(last.temperature)),
        "abs_pressure_delta": abs(float(r.pressure) - float(last.pressure)),
        "abs_humidity_delta": abs(float(r.humidity) - float(last.humidity)),
        "temp_std5": float(np.std(hist.temperature.astype(float))) if len(hist) else 1.0,
        "pressure_std5": float(np.std(hist.pressure.astype(float))) if len(hist) else 1.0,
        "humidity_std5": float(np.std(hist.humidity.astype(float))) if len(hist) else 1.0,
        "temp_out_of_range": int(float(r.temperature) < -40 or float(r.temperature) > 60),
        "pressure_out_of_range": int(float(r.pressure) < 800 or float(r.pressure) > 1100),
        "humidity_out_of_range": int(float(r.humidity) < 0 or float(r.humidity) > 100),
    }


def main():
    df = pd.read_csv(CSV)
    df["date_parsed"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    df = df.sort_values(["station", "date_parsed"]).reset_index(drop=True)

    normal_rows = []
    for _, group in df.groupby("station"):
        group = group.reset_index(drop=True)
        for i, row in group.iterrows():
            normal_rows.append(features_for_row(row, group.iloc[max(0, i - 5):i]))
    normal = pd.DataFrame(normal_rows)

    hard_rows = []
    for _ in range(len(normal) * 2):
        base = normal.iloc[int(RNG.integers(len(normal)))].copy()
        kind = int(RNG.integers(5))
        if kind == 0:
            base["temperature"] = float(RNG.choice([RNG.uniform(65, 120), RNG.uniform(-90, -41)]))
            base["temp_out_of_range"] = 1
        elif kind == 1:
            base["pressure"] = float(RNG.choice([RNG.uniform(1101, 1400), RNG.uniform(400, 799)]))
            base["pressure_out_of_range"] = 1
        elif kind == 2:
            base["humidity"] = float(RNG.choice([RNG.uniform(101, 140), RNG.uniform(-30, -1)]))
            base["humidity_out_of_range"] = 1
        elif kind == 3:
            base["abs_temp_delta"] = RNG.uniform(15, 60)
            base["abs_pressure_delta"] = RNG.uniform(20, 150)
            base["abs_humidity_delta"] = RNG.uniform(25, 90)
        else:
            base["abs_temp_delta"] = RNG.uniform(0, 0.00001)
            base["abs_pressure_delta"] = RNG.uniform(0, 0.00001)
            base["abs_humidity_delta"] = RNG.uniform(0, 0.00001)
            base["temp_std5"] = RNG.uniform(0, 0.00001)
            base["pressure_std5"] = RNG.uniform(0, 0.00001)
            base["humidity_std5"] = RNG.uniform(0, 0.00001)
        hard_rows.append(base)
    hard = pd.DataFrame(hard_rows)

    features = list(normal.columns)
    X = pd.concat([normal, hard], ignore_index=True)[features].astype(float)
    y = np.r_[np.zeros(len(normal), dtype=int), np.ones(len(hard), dtype=int)]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, stratify=y, random_state=42)

    model = DecisionTreeClassifier(max_depth=6, min_samples_leaf=10, class_weight="balanced", random_state=42)
    model.fit(X_train, y_train)

    tree = model.tree_

    def export_node(index: int):
        if tree.children_left[index] == tree.children_right[index]:
            probs = tree.value[index][0] / tree.weighted_n_node_samples[index]
            return {"leaf": True, "prob_normal": float(probs[0]), "prob_hard": float(probs[1])}
        return {
            "leaf": False,
            "feature": features[int(tree.feature[index])],
            "threshold": float(tree.threshold[index]),
            "left": export_node(int(tree.children_left[index])),
            "right": export_node(int(tree.children_right[index])),
        }

    payload = {
        "model_type": "portable_decision_tree",
        "purpose": "edge_hard_anomaly_gate",
        "features": features,
        "max_depth": int(model.tree_.max_depth),
        "test_accuracy": float(model.score(X_test, y_test)),
        "training_note": "Offline-trained on historical AWS normal observations plus simulated hard faults: out-of-range values, abrupt spikes and frozen channels.",
        "tree": export_node(0),
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {OUT}")
    print(f"Test accuracy: {payload['test_accuracy']:.4f}")


if __name__ == "__main__":
    main()
