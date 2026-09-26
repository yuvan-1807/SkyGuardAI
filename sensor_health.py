"""
Sensor Health Prediction Layer
Predict sensor degradation and maintenance requirements
Tracks drift patterns and estimates time-to-failure
"""

from config import DB_PATH

import numpy as np
import sqlite3
from datetime import datetime, timedelta

class SensorHealthPredictor:
    """
    Monitor sensor health and predict maintenance needs
    Detects drift patterns and estimates degradation timeline
    """
    
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.health_scores = {}  # {station: health_percentage}
        self.degradation_rates = {}  # {station: {param: drift_rate}}
    
    def calculate_sensor_health_score(self, station_name, num_readings=100):
        """
        Calculate overall sensor health (0-100%)
        Based on anomaly rate, drift detection, and consistency
        
        Returns:
            dict: Health metrics for station
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get recent readings
            cursor.execute("""
                SELECT temperature, pressure, humidity, is_anomaly, label
                FROM sensor_readings
                WHERE station_name = ?
                ORDER BY created_at DESC
                LIMIT ?
            """, (station_name, num_readings))
            
            readings = cursor.fetchall()
            conn.close()
            
            if not readings:
                return {'station': station_name, 'health_score': 50, 'status': 'Unknown'}
            
            readings.reverse()  # Chronological order
            
            # Extract parameters
            temps = [r[0] for r in readings]
            pressures = [r[1] for r in readings]
            humidities = [r[2] for r in readings]
            anomalies = [r[3] for r in readings]
            
            # Calculate health components
            health_score = 100
            
            # 1. Anomaly rate (20 points max deduction)
            anomaly_rate = sum(anomalies) / len(anomalies) if anomalies else 0
            health_score -= anomaly_rate * 20
            
            # 2. Drift detection (15 points max deduction)
            temp_drift = self._detect_drift(temps)
            press_drift = self._detect_drift(pressures)
            humid_drift = self._detect_drift(humidities)
            
            max_drift = max(temp_drift, press_drift, humid_drift)
            health_score -= min(15, max_drift * 100)
            
            # 3. Variance consistency (10 points max deduction)
            first_half = temps[:len(temps)//2]
            second_half = temps[len(temps)//2:]
            
            var_change = abs(np.var(second_half) - np.var(first_half)) / (np.var(first_half) + 1e-10)
            health_score -= min(10, var_change * 5)
            
            # 4. Freeze detection (15 points max deduction)
            freeze_penalty = self._detect_freeze(temps) * 15
            health_score -= freeze_penalty
            
            health_score = max(0, min(100, health_score))
            
            # Determine status
            if health_score >= 90:
                status = "Excellent"
                color = "Green"
            elif health_score >= 70:
                status = "Good"
                color = "Green"
            elif health_score >= 50:
                status = "Degrading"
                color = "Yellow"
            elif health_score >= 30:
                status = "Poor"
                color = "Orange"
            else:
                status = "Critical"
                color = "Red"
            
            return {
                'station': station_name,
                'health_score': health_score,
                'status': status,
                'color': color,
                'anomaly_rate': anomaly_rate * 100,
                'drift_score': max_drift,
                'num_readings_analyzed': len(readings)
            }
        
        except:
            return {'station': station_name, 'health_score': 50, 'status': 'Error'}
    
    def _detect_drift(self, readings, window=20):
        """
        Detect calibration drift (slow systematic change)
        Returns drift magnitude (0-1)
        """
        if len(readings) < window:
            return 0
        
        # Split into two halves
        first_half = readings[:len(readings)//2]
        second_half = readings[len(readings)//2:]
        
        # Compare means
        mean_change = abs(np.mean(second_half) - np.mean(first_half))
        std_dev = np.std(readings)
        
        if std_dev < 1e-6:
            return 0
        
        # Drift is mean change normalized by standard deviation
        drift = mean_change / (std_dev + 1e-10)
        
        return min(1.0, drift)
    
    def _detect_freeze(self, readings, window=5):
        """
        Detect frozen sensor (zero variance over window)
        Returns freeze score (0-1)
        """
        if len(readings) < window:
            return 0
        
        for i in range(len(readings) - window):
            window_data = readings[i:i+window]
            variance = np.var(window_data)
            
            if variance < 1e-6:  # Essentially zero
                return 1.0
        
        return 0
    
    def predict_maintenance_timeline(self, station_name):
        """
        Predict when maintenance will be needed
        Based on degradation rate and health trend
        
        Returns:
            dict: Maintenance prediction
        """
        health = self.calculate_sensor_health_score(station_name)
        
        health_score = health['health_score']
        
        # Estimate days until failure based on health score
        if health_score >= 80:
            days_until_maintenance = 365  # Over a year
            urgency = "Low"
            recommendation = "Schedule routine maintenance within 12 months"
        elif health_score >= 60:
            days_until_maintenance = 90
            urgency = "Medium"
            recommendation = "Schedule maintenance within 3 months"
        elif health_score >= 40:
            days_until_maintenance = 30
            urgency = "High"
            recommendation = "Schedule maintenance within 1 month"
        else:
            days_until_maintenance = 7
            urgency = "Critical"
            recommendation = "Urgent: Service required within 1 week"
        
        maintenance_date = datetime.now() + timedelta(days=days_until_maintenance)
        
        return {
            'station': station_name,
            'health_score': health_score,
            'days_until_maintenance': days_until_maintenance,
            'maintenance_date': maintenance_date.strftime('%Y-%m-%d'),
            'urgency': urgency,
            'recommendation': recommendation,
            'drift_rate': health.get('drift_score', 0),
            'anomaly_rate': health.get('anomaly_rate', 0)
        }
    
    def get_all_health_status(self):
        """
        Get health status for all 10 stations
        For dashboard overview
        """
        stations = [
            'AWS_01_Delhi',
            'AWS_02_Mumbai',
            'AWS_03_Chennai',
            'AWS_04_Himachal',
            'AWS_05_Kolkata',
            'AWS_06_Bangalore',
            'AWS_07_Kerala',
            'AWS_08_Rajasthan',
            'AWS_09_NorthEast',
            'AWS_10_Gujarat'
        ]
        
        health_status = []
        
        for station in stations:
            health = self.calculate_sensor_health_score(station)
            maintenance = self.predict_maintenance_timeline(station)
            
            health_status.append({
                'station': station,
                'health_score': health['health_score'],
                'status': health['status'],
                'color': health.get('color', 'Gray'),
                'maintenance_urgency': maintenance['urgency'],
                'maintenance_date': maintenance['maintenance_date'],
                'anomaly_rate': health.get('anomaly_rate', 0),
                'recommendation': maintenance['recommendation']
            })
        
        return sorted(health_status, key=lambda x: x['health_score'])
    
    def get_degradation_report(self, station_name, days=30):
        """
        Generate detailed degradation report over time period
        Shows trend of sensor health decline
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get readings over time period
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            cursor.execute("""
                SELECT DATE(timestamp), COUNT(*), 
                       SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END)
                FROM sensor_readings
                WHERE station_name = ? AND timestamp >= ?
                GROUP BY DATE(timestamp)
                ORDER BY DATE(timestamp)
            """, (station_name, start_date))
            
            daily_data = cursor.fetchall()
            conn.close()
            
            # Calculate daily health
            daily_health = []
            for date, total, anomalies in daily_data:
                anomaly_rate = (anomalies / total * 100) if total > 0 else 0
                health = 100 - anomaly_rate
                daily_health.append({
                    'date': date,
                    'health_score': health,
                    'anomalies': anomalies,
                    'total_readings': total
                })
            
            # Calculate trend
            if len(daily_health) >= 2:
                first_week = np.mean([d['health_score'] for d in daily_health[:7]])
                last_week = np.mean([d['health_score'] for d in daily_health[-7:]])
                trend = last_week - first_week
                
                if trend < -5:
                    trend_status = "Rapidly Degrading"
                elif trend < 0:
                    trend_status = "Slowly Degrading"
                elif trend > 5:
                    trend_status = "Improving"
                else:
                    trend_status = "Stable"
            else:
                trend = 0
                trend_status = "Insufficient Data"
            
            return {
                'station': station_name,
                'period_days': days,
                'daily_health': daily_health,
                'trend': trend,
                'trend_status': trend_status,
                'current_health': daily_health[-1]['health_score'] if daily_health else 0
            }
        
        except:
            return {'station': station_name, 'error': 'Unable to calculate degradation'}
    
    def estimate_remaining_lifespan(self, station_name):
        """
        Estimate remaining lifespan based on degradation curve
        Typical sensor lifespan: 2-5 years
        """
        degradation = self.get_degradation_report(station_name, days=30)
        
        if 'error' in degradation:
            return {'estimate': 'Unknown', 'confidence': 'Low'}
        
        trend = degradation['trend']
        current_health = degradation['current_health']
        
        # Extrapolate: if health decreases at current rate, when hits 0?
        if trend < -1:  # Significant degradation
            days_to_failure = abs(current_health / trend)
            years_to_failure = days_to_failure / 365
            
            if years_to_failure > 2:
                estimate = f"~{years_to_failure:.1f} years"
                confidence = "Medium"
            else:
                estimate = f"~{int(days_to_failure)} days"
                confidence = "High"
        else:
            estimate = ">2 years"
            confidence = "Low"
        
        return {
            'station': station_name,
            'estimate': estimate,
            'confidence': confidence,
            'current_health': current_health,
            'degradation_rate': trend
        }
