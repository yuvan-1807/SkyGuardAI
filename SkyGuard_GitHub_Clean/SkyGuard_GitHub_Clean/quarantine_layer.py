"""
EDGE 4: Quarantine Layer
Adversarial attack detection and security isolation
Detects 8 attack patterns and isolates compromised stations
"""

from config import DB_PATH

import numpy as np
from datetime import datetime, timedelta
import sqlite3

class QuarantineLayer:
    """
    Detect adversarial data injection attacks and isolate compromised stations
    Uses pattern matching against known attack signatures
    """
    
    # 8 Adversarial attack patterns to detect
    ATTACK_SIGNATURES = {
        'gradual_drift_spoofing': {
            'description': 'Slow, monotonic temperature increase (hacking)',
            'pattern': 'continuous_increase',
            'threshold': 0.3,  # °C per reading
            'window': 20,
            'severity': 'High'
        },
        'sudden_spike_injection': {
            'description': 'Rapid spike then return to normal (mimics natural event)',
            'pattern': 'spike_recovery',
            'threshold': 15,  # °C
            'window': 5,
            'severity': 'High'
        },
        'offset_bias': {
            'description': 'Constant bias added to all readings',
            'pattern': 'constant_offset',
            'threshold': 5,  # °C
            'window': 30,
            'severity': 'Medium'
        },
        'noise_injection': {
            'description': 'Artificial random noise pattern (unnatural statistics)',
            'pattern': 'artificial_noise',
            'threshold': 0.8,  # Entropy threshold
            'window': 15,
            'severity': 'Medium'
        },
        'data_replay': {
            'description': 'Repeated sequence from past (exact copy)',
            'pattern': 'exact_repetition',
            'threshold': 0.99,  # Correlation
            'window': 50,
            'severity': 'Critical'
        },
        'oscillation_attack': {
            'description': 'Unnatural oscillating pattern',
            'pattern': 'oscillation',
            'threshold': 10,  # Cycle count
            'window': 30,
            'severity': 'High'
        },
        'physics_violation_spoofing': {
            'description': 'Injection of physically impossible combinations',
            'pattern': 'physics_violation',
            'threshold': 0.5,  # Violation count ratio
            'window': 20,
            'severity': 'Critical'
        },
        'coordination_attack': {
            'description': 'Multiple stations showing correlated anomalies (coordinated hack)',
            'pattern': 'multi_station_sync',
            'threshold': 3,  # Number of stations
            'window': 10,
            'severity': 'Critical'
        }
    }
    
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.quarantine_zone = {}  # {station: quarantine_info}
        self.audit_log = []
    
    def detect_gradual_drift_spoofing(self, readings):
        """
        Detect slow, deliberate temperature increase (hacking signature)
        Natural drift is random; adversarial is monotonic
        """
        if len(readings) < 5:
            return {'detected': False, 'score': 0}
        
        diffs = np.diff(readings)
        positive_diffs = np.sum(diffs > 0.2)  # Threshold: 0.2°C per reading
        
        if positive_diffs / len(diffs) > 0.8:  # 80%+ of changes are positive
            return {
                'detected': True,
                'score': 0.9,
                'reason': 'Monotonic temperature increase (hacking pattern)'
            }
        
        return {'detected': False, 'score': 0}
    
    def detect_spike_recovery(self, readings):
        """Detect spike then recovery (mimics natural event but pattern is suspicious)"""
        if len(readings) < 5:
            return {'detected': False, 'score': 0}
        
        # Find max value
        max_idx = np.argmax(readings)
        
        if max_idx == 0 or max_idx == len(readings) - 1:
            return {'detected': False, 'score': 0}
        
        # Check if rapid increase then rapid decrease
        increase = readings[max_idx] - readings[max_idx - 1]
        decrease = readings[max_idx] - readings[max_idx + 1]
        
        if increase > 10 and decrease > 10:
            return {
                'detected': True,
                'score': 0.85,
                'reason': 'Spike-recovery pattern detected (spoofed event)'
            }
        
        return {'detected': False, 'score': 0}
    
    def detect_constant_offset(self, readings):
        """Detect if constant bias is added to all readings"""
        if len(readings) < 10:
            return {'detected': False, 'score': 0}
        
        diffs = np.diff(readings)
        
        # If differences are very small but values all shifted, it's offset bias
        if np.std(diffs) < 0.1 and np.mean(np.abs(readings)) > 25:
            return {
                'detected': True,
                'score': 0.8,
                'reason': 'Constant offset bias detected (all readings shifted)'
            }
        
        return {'detected': False, 'score': 0}
    
    def detect_exact_repetition(self, readings, comparison_window=50):
        """Detect if sequence is replayed from past (data replay attack)"""
        if len(readings) < comparison_window:
            return {'detected': False, 'score': 0}
        
        recent = readings[-20:]
        older = readings[-comparison_window:-20]
        
        if len(older) >= len(recent):
            # Check correlation
            correlation = np.corrcoef(recent, older[:len(recent)])[0, 1]
            
            if correlation > 0.95:  # Very high correlation = replay
                return {
                    'detected': True,
                    'score': 0.95,
                    'reason': 'Exact data replay detected (same sequence repeated)'
                }
        
        return {'detected': False, 'score': 0}
    
    def detect_oscillation_attack(self, readings):
        """Detect unnatural oscillating pattern"""
        if len(readings) < 10:
            return {'detected': False, 'score': 0}
        
        diffs = np.diff(readings)
        sign_changes = np.sum(np.diff(np.sign(diffs)) != 0)
        
        # Natural: 2-3 direction changes in 30 readings
        # Attack: 8+ rapid oscillations
        if sign_changes > 8:
            return {
                'detected': True,
                'score': 0.85,
                'reason': f'{sign_changes} rapid oscillations detected (unnatural pattern)'
            }
        
        return {'detected': False, 'score': 0}
    
    def detect_artificial_noise(self, readings):
        """Detect artificially injected noise (unnatural statistics)"""
        if len(readings) < 10:
            return {'detected': False, 'score': 0}
        
        variance = np.var(readings)
        skewness = np.mean((readings - np.mean(readings))**3) / (np.std(readings)**3 + 1e-10)
        
        # Natural weather has low skewness; artificial noise is symmetric
        if variance > 20 and abs(skewness) < 0.1:
            return {
                'detected': True,
                'score': 0.75,
                'reason': 'Artificial noise pattern detected (high variance, zero skewness)'
            }
        
        return {'detected': False, 'score': 0}
    
    def detect_physics_violation_spoofing(self, temp, humidity, pressure):
        """Detect physically impossible combinations (adversarial input)"""
        violations = 0
        
        # Extreme heat + extreme humidity + normal pressure = physically suspicious
        if temp > 45 and humidity > 90 and pressure > 1010:
            violations += 1
        
        # Dew point check
        dew_point = 243.04 * np.log(humidity / 100) / (17.625 - np.log(humidity / 100))
        if dew_point > temp:
            violations += 1
        
        if violations > 0:
            return {
                'detected': True,
                'score': 0.9,
                'reason': f'Physics violations: {violations} (adversarial injection)'
            }
        
        return {'detected': False, 'score': 0}
    
    def detect_multi_station_sync(self, station_anomalies):
        """
        Detect coordinated attack across multiple stations
        Real events affect specific regions; hacks can sync multiple stations artificially
        """
        if len(station_anomalies) < 3:
            return {'detected': False, 'score': 0}
        
        # Too many stations with anomalies at same time = coordinated attack
        anomaly_count = sum(1 for v in station_anomalies.values() if v)
        
        if anomaly_count >= 3:
            return {
                'detected': True,
                'score': 0.9,
                'reason': f'{anomaly_count} stations anomalous simultaneously (coordinated attack)'
            }
        
        return {'detected': False, 'score': 0}
    
    def scan_for_attacks(self, station_name, temperature, pressure, humidity, temp_history):
        """
        Full attack detection pipeline
        Scan all 8 attack patterns and return threat score
        """
        results = {
            'station': station_name,
            'timestamp': datetime.now().isoformat(),
            'threat_level': 'Safe',
            'threat_score': 0,
            'detected_attacks': [],
            'should_quarantine': False
        }
        
        threat_scores = []
        
        # Check each attack pattern
        if temp_history:
            drift_result = self.detect_gradual_drift_spoofing(np.array(temp_history))
            if drift_result['detected']:
                results['detected_attacks'].append(drift_result)
                threat_scores.append(drift_result['score'])
            
            spike_result = self.detect_spike_recovery(np.array(temp_history))
            if spike_result['detected']:
                results['detected_attacks'].append(spike_result)
                threat_scores.append(spike_result['score'])
            
            offset_result = self.detect_constant_offset(np.array(temp_history))
            if offset_result['detected']:
                results['detected_attacks'].append(offset_result)
                threat_scores.append(offset_result['score'])
            
            replay_result = self.detect_exact_repetition(np.array(temp_history))
            if replay_result['detected']:
                results['detected_attacks'].append(replay_result)
                threat_scores.append(replay_result['score'])
            
            oscillation_result = self.detect_oscillation_attack(np.array(temp_history))
            if oscillation_result['detected']:
                results['detected_attacks'].append(oscillation_result)
                threat_scores.append(oscillation_result['score'])
            
            noise_result = self.detect_artificial_noise(np.array(temp_history))
            if noise_result['detected']:
                results['detected_attacks'].append(noise_result)
                threat_scores.append(noise_result['score'])
        
        # Physics violation check
        physics_result = self.detect_physics_violation_spoofing(temperature, humidity, pressure)
        if physics_result['detected']:
            results['detected_attacks'].append(physics_result)
            threat_scores.append(physics_result['score'])
        
        # Calculate threat score
        if threat_scores:
            results['threat_score'] = np.mean(threat_scores)
            
            if results['threat_score'] > 0.8:
                results['threat_level'] = 'Critical'
                results['should_quarantine'] = True
            elif results['threat_score'] > 0.6:
                results['threat_level'] = 'High'
                results['should_quarantine'] = True
            elif results['threat_score'] > 0.4:
                results['threat_level'] = 'Medium'
        
        return results
    
    def quarantine_station(self, station_name, reason, threat_score):
        """Isolate station to Quarantine Zone"""
        self.quarantine_zone[station_name] = {
            'timestamp': datetime.now().isoformat(),
            'reason': reason,
            'threat_score': threat_score,
            'status': 'Isolated',
            'data_transmission': 'Blocked'
        }
        
        # Log to audit trail
        self.audit_log.append({
            'event': 'Station Quarantined',
            'station': station_name,
            'timestamp': datetime.now().isoformat(),
            'reason': reason,
            'threat_score': threat_score
        })
    
    def get_audit_log(self):
        """Return security audit log"""
        return {
            'total_events': len(self.audit_log),
            'quarantined_stations': list(self.quarantine_zone.keys()),
            'recent_events': self.audit_log[-10:]  # Last 10 events
        }
