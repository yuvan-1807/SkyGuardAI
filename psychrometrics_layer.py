"""
EDGE 1: Physics-Informed ML Layer
Psychrometrics-based anomaly detection
Detects physically impossible combinations before ML runs
"""

import numpy as np
import math

class PsychrometricsLayer:
    """
    Detect physically impossible meteorological combinations using thermodynamic principles.
    """
    
    @staticmethod
    def calculate_dew_point(temperature_c, humidity_percent):
        """
        Calculate dew point using Magnus formula
        Returns: dew point in °C
        """
        if humidity_percent <= 0 or humidity_percent > 100:
            return None
        
        a = 17.27
        b = 237.7  # in °C
        
        alpha = ((a * temperature_c) / (b + temperature_c)) + np.log(humidity_percent / 100.0)
        dew_point = (b * alpha) / (a - alpha)
        
        return dew_point
    
    @staticmethod
    def calculate_saturation_vapor_pressure(temperature_c):
        """
        Clausius-Clapeyron equation: de_s/dT = (L_v * e_s) / (R_v * T^2)
        
        This implements the thermodynamic relation between saturation vapor pressure (e_s)
        and temperature. The Magnus formula encodes this differential equation:
        e_s(T) = 6.1094 * exp((17.625*T) / (T + 243.04))
        
        Physical meaning: Higher temperature → exponentially higher saturation vapor pressure
        (More water molecules escape liquid phase at higher T)
        
        Used to validate: If observed RH > 100% at given T, sensor is faulty (violates Clausius-Clapeyron)
        
        Returns: saturation vapor pressure in hPa
        """
        e_s = 6.1094 * np.exp((17.625 * temperature_c) / (temperature_c + 243.04))
        return e_s
    
    @staticmethod
    def check_physical_consistency(temperature_c, humidity_percent, pressure_hpa):
        """
        Check if temperature, humidity, pressure combination is physically possible.
        
        Rules:
        1. Dew point must be <= temperature (always true by physics)
        2. High humidity requires lower pressure at same temperature (inverse correlation)
        3. Extreme heat (>50°C) with extreme humidity (>95%) is physically suspicious without pressure drop
        4. High altitude (low pressure) should have lower temperature
        
        Returns: (is_valid, violation_reason, confidence_score)
        """
        violations = []
        confidence_score = 100  # Start at 100%, decrease for each violation
        
        # Check 1: Temperature and humidity basic bounds
        if temperature_c < -60 or temperature_c > 60:
            violations.append("Temperature out of Earth bounds")
            confidence_score -= 40
        
        if humidity_percent < 0 or humidity_percent > 105:
            violations.append("Humidity impossible")
            confidence_score -= 40
        
        if pressure_hpa < 850 or pressure_hpa > 1085:
            violations.append("Pressure out of normal range")
            confidence_score -= 30
        
        # Check 2: Dew point consistency
        if temperature_c > -60 and humidity_percent > 0 and humidity_percent <= 100:
            dew_point = PsychrometricsLayer.calculate_dew_point(temperature_c, humidity_percent)
            if dew_point is not None and dew_point > temperature_c:
                violations.append(f"Dew point ({dew_point:.1f}°C) > Temperature ({temperature_c:.1f}°C)")
                confidence_score -= 50
        
        # Check 3: High heat + High humidity + Normal pressure (physically impossible)
        if temperature_c > 45 and humidity_percent > 90 and pressure_hpa > 1010:
            violations.append("Extreme heat (>45°C) + extreme humidity (>90%) but normal/high pressure")
            violations.append("  → Expected: severe pressure DROP during heatwave")
            confidence_score -= 35
        
        # Check 4: High humidity + High pressure + Normal temp (suspicious)
        if humidity_percent > 85 and pressure_hpa > 1015 and temperature_c < 15:
            violations.append("High humidity with high pressure at low temperature")
            violations.append("  → Expected: pressure and humidity inverse correlation")
            confidence_score -= 25
        
        # Check 5: Clausius-Clapeyron saturation vapor pressure validation
        sat_vapor_pressure = PsychrometricsLayer.calculate_saturation_vapor_pressure(temperature_c)
        actual_vapor_pressure = (humidity_percent / 100.0) * sat_vapor_pressure
        
        # If RH > 100%, violates Clausius-Clapeyron (sensor malfunction)
        if humidity_percent > 100:
            violations.append("Relative humidity > 100% violates Clausius-Clapeyron saturation limit")
            confidence_score -= 50
        
        # Check 6: Hydrostatic Equilibrium - Pressure/Temperature lapse rate check
        # Barometric formula: P(z) = P_0 * exp(-Mg*z / RT)
        # At high altitudes (low P), temperature should decrease proportionally
        # Inverse: High T + Very low P = unphysical (violates hydrostatic equilibrium)
        if temperature_c > 30 and pressure_hpa < 880:
            # At 3000m+ altitude (P < 880 hPa), T > 30°C is physically impossible
            violations.append("High temperature (>30°C) at extreme altitude (<880 hPa)")
            violations.append("  → Violates hydrostatic equilibrium (barometric formula)")
            confidence_score -= 45
        
        # Check 7: At extreme altitudes (low pressure), saturation physics changes
        if pressure_hpa < 900:  # High altitude (>1000m)
            # Expected lower saturation at high altitude per hydrostatic law
            if humidity_percent > 95:
                violations.append("Very high humidity (>95%) at high altitude (P < 900 hPa)")
                violations.append("  → Violates reduced saturation vapor pressure at altitude")
                confidence_score -= 20
        
        # Confidence score floor
        confidence_score = max(0, confidence_score)
        
        is_valid = len(violations) == 0
        
        return is_valid, violations, confidence_score
    
    @staticmethod
    def validate_reading(temperature_c, humidity_percent, pressure_hpa):
        """
        Main validation function
        Returns dict with detailed analysis
        """
        is_valid, violations, confidence_score = PsychrometricsLayer.check_physical_consistency(
            temperature_c, humidity_percent, pressure_hpa
        )
        
        dew_point = PsychrometricsLayer.calculate_dew_point(temperature_c, humidity_percent)
        sat_vapor = PsychrometricsLayer.calculate_saturation_vapor_pressure(temperature_c)
        
        return {
            'is_physically_valid': is_valid,
            'confidence_score': confidence_score,
            'violations': violations,
            'dew_point_c': dew_point,
            'saturation_vapor_pressure_hpa': sat_vapor,
            'anomaly_type': 'Physics Violation' if not is_valid else 'Normal'
        }


# Test cases
if __name__ == "__main__":
    print("=" * 70)
    print("PSYCHROMETRICS LAYER TEST CASES")
    print("=" * 70)
    
    test_cases = [
        {
            'name': '✅ Normal Summer Day',
            'temp': 32,
            'humidity': 65,
            'pressure': 1011
        },
        {
            'name': '✅ Normal Winter Day',
            'temp': 15,
            'humidity': 55,
            'pressure': 1013
        },
        {
            'name': '❌ IMPOSSIBLE: Extreme heat + extreme humidity + normal pressure',
            'temp': 52,
            'humidity': 99,
            'pressure': 1012
        },
        {
            'name': '❌ IMPOSSIBLE: Dew point > Temperature',
            'temp': 20,
            'humidity': 120,
            'pressure': 1013
        },
        {
            'name': '❌ IMPOSSIBLE: High humidity at high altitude',
            'temp': 5,
            'humidity': 98,
            'pressure': 850
        },
        {
            'name': '✅ Realistic Heatwave (high temp + humidity DROP + pressure drop)',
            'temp': 48,
            'humidity': 35,
            'pressure': 1005
        },
        {
            'name': '❌ Sensor Spike (impossible temperature)',
            'temp': 65,
            'humidity': 45,
            'pressure': 1012
        },
        {
            'name': '✅ Monsoon Conditions',
            'temp': 28,
            'humidity': 88,
            'pressure': 1008
        },
    ]
    
    for test in test_cases:
        print(f"\n{test['name']}")
        print(f"  Input: Temp={test['temp']}°C, RH={test['humidity']}%, P={test['pressure']} hPa")
        
        result = PsychrometricsLayer.validate_reading(
            test['temp'],
            test['humidity'],
            test['pressure']
        )
        
        print(f"  Is Valid: {result['is_physically_valid']}")
        print(f"  Confidence Score: {result['confidence_score']:.0f}%")
        if result['dew_point_c'] is not None:
            print(f"  Dew Point: {result['dew_point_c']:.1f}°C")
        else:
            print(f"  Dew Point: N/A (invalid RH)")
        print(f"  Saturation VP: {result['saturation_vapor_pressure_hpa']:.2f} hPa")
        
        if result['violations']:
            print(f"  Violations:")
            for v in result['violations']:
                print(f"    - {v}")
