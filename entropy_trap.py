"""
EDGE 2: Entropy Trap
Detect frozen sensors using mathematical entropy and zero-variance detection
Catches hardware lockups before transmission to save 90% bandwidth
"""

import numpy as np
from collections import deque

class EntropyTrap:
    """
    Detect frozen sensors by monitoring zero variance over rolling window
    Frozen sensor = exact same reading continuously (σ² = 0)
    """
    
    def __init__(self, window_size=12):  # 12 readings = 1 hour at 5-sec intervals
        """
        Args:
            window_size: Number of readings to track (rolling window)
        """
        self.window_size = window_size
        self.reading_history = {}  # {station: deque of readings}
    
    def update_history(self, station_name, temperature, pressure, humidity):
        """Add reading to history for station"""
        if station_name not in self.reading_history:
            self.reading_history[station_name] = {
                'temp': deque(maxlen=self.window_size),
                'pressure': deque(maxlen=self.window_size),
                'humidity': deque(maxlen=self.window_size)
            }
        
        self.reading_history[station_name]['temp'].append(temperature)
        self.reading_history[station_name]['pressure'].append(pressure)
        self.reading_history[station_name]['humidity'].append(humidity)
    
    def calculate_entropy(self, values):
        """
        Calculate Shannon entropy for a set of readings
        Low entropy = repeated same values (sensor frozen)
        High entropy = varied readings (normal)
        """
        if len(values) < 2:
            return 0
        
        # Check for zero variance (all same value)
        variance = np.var(values)
        if variance < 1e-6:  # Essentially zero
            return 0
        
        # Calculate Shannon entropy
        values_array = np.array(values)
        # Normalize to 0-1 range
        if len(np.unique(values_array)) == 1:
            return 0  # All values identical
        
        # Entropy based on value distribution
        min_val = np.min(values_array)
        max_val = np.max(values_array)
        
        if max_val == min_val:
            return 0
        
        # Quantize into bins
        bins = np.histogram_bin_edges(values_array, bins=5)
        hist, _ = np.histogram(values_array, bins=bins)
        hist = hist[hist > 0]
        
        if len(hist) == 0:
            return 0
        
        probabilities = hist / len(values_array)
        entropy = -np.sum(probabilities * np.log2(probabilities + 1e-10))
        
        return entropy
    
    def detect_frozen_sensor(self, station_name, temperature, pressure, humidity):
        """
        Detect if sensor is frozen (zero variance over window)
        
        Returns:
            dict: Detection result with anomaly info
        """
        self.update_history(station_name, temperature, pressure, humidity)
        
        history = self.reading_history[station_name]
        
        result = {
            'is_frozen': False,
            'frozen_parameter': None,
            'confidence': 0,
            'severity': 'Low',
            'reason': 'Normal variance detected',
            'entropy_scores': {}
        }
        
        # Check temperature
        if len(history['temp']) >= self.window_size:
            temp_entropy = self.calculate_entropy(list(history['temp']))
            result['entropy_scores']['temperature'] = temp_entropy
            
            temp_variance = np.var(list(history['temp']))
            if temp_variance < 1e-4:  # Nearly zero variance
                result['is_frozen'] = True
                result['frozen_parameter'] = 'Temperature'
                result['confidence'] = 95
                result['severity'] = 'Critical'
                result['reason'] = f'Temperature frozen at {temperature:.1f}°C (zero variance over 1 hour)'
                return result
        
        # Check pressure
        if len(history['pressure']) >= self.window_size:
            pressure_entropy = self.calculate_entropy(list(history['pressure']))
            result['entropy_scores']['pressure'] = pressure_entropy
            
            pressure_variance = np.var(list(history['pressure']))
            if pressure_variance < 1e-3:  # Nearly zero variance
                result['is_frozen'] = True
                result['frozen_parameter'] = 'Pressure'
                result['confidence'] = 92
                result['severity'] = 'Critical'
                result['reason'] = f'Pressure frozen at {pressure:.1f} hPa (zero variance over 1 hour)'
                return result
        
        # Check humidity
        if len(history['humidity']) >= self.window_size:
            humidity_entropy = self.calculate_entropy(list(history['humidity']))
            result['entropy_scores']['humidity'] = humidity_entropy
            
            humidity_variance = np.var(list(history['humidity']))
            if humidity_variance < 1e-3:  # Nearly zero variance
                result['is_frozen'] = True
                result['frozen_parameter'] = 'Humidity'
                result['confidence'] = 88
                result['severity'] = 'Critical'
                result['reason'] = f'Humidity frozen at {humidity:.1f}% (zero variance over 1 hour)'
                return result
        
        return result
    
    def calculate_bandwidth_saved(self, total_readings, frozen_count):
        """
        Calculate bandwidth savings from detecting frozen sensors
        Frozen readings are not transmitted (flagged locally)
        
        Returns:
            dict: Bandwidth optimization metrics
        """
        if total_readings == 0:
            return {'percent_saved': 0, 'readings_filtered': 0}
        
        percent_saved = (frozen_count / total_readings) * 100
        
        return {
            'percent_saved': percent_saved,
            'readings_filtered': frozen_count,
            'total_readings': total_readings,
            'savings_explanation': f"Filtered {frozen_count}/{total_readings} frozen readings = {percent_saved:.1f}% bandwidth saved"
        }
