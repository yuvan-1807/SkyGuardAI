// PHASE 2D SECURITY REFERENCE: production firmware should use a unique per-device secret, HMAC-SHA256, sequence numbers, durable LittleFS/FRAM/SD queue, ACK-based deletion, secure boot and flash encryption where supported.
/*
  SkyGuard AI - ESP32 Edge Hard-Anomaly Gate (reference firmware)

  Purpose:
    - Perform cheap local checks before radio transmission.
    - BLOCK obvious hard anomalies locally.
    - Buffer trusted observations and transmit a batch every 5/10 minutes.
    - Send a compact anomaly event immediately (small packet) so operators still
      see the fault without uploading the full bad observation to the backend.

  The Python edge emulator uses the same decision policy. Replace the demo
  sensor() function with your actual temperature/pressure/RH sensor drivers.
*/
#include <Arduino.h>
#include <math.h>

struct Reading {
  float temperature;
  float pressure;
  float humidity;
};

static const float TEMP_MIN = -40.0f, TEMP_MAX = 60.0f;
static const float PRESS_MIN = 800.0f, PRESS_MAX = 1100.0f;
static const float RH_MIN = 0.0f, RH_MAX = 100.0f;
static const float TEMP_JUMP = 12.0f;
static const float PRESS_JUMP = 20.0f;
static const float RH_JUMP = 30.0f;
static const int WINDOW = 5;

Reading history[WINDOW];
int historyCount = 0;

bool finiteReading(const Reading &r) {
  return isfinite(r.temperature) && isfinite(r.pressure) && isfinite(r.humidity);
}

struct EdgeFeatures {
  float temperature, pressure, humidity;
  float absTempDelta, absPressureDelta, absHumidityDelta;
  float tempStd5, pressureStd5, humidityStd5;
  int tempOutOfRange, pressureOutOfRange, humidityOutOfRange;
};

// Exported from edge_ai_model.json; inference uses only comparisons (no ML library).
float portableTreeHardProbability(const EdgeFeatures &f) {
  if (f.pressure <= 946.9461669922f) {
    if (f.pressure <= 946.2498779297f) {
      return 0.0005860806f;
    } else {
      return 0.0991735537f;
    }
  } else {
    if (f.humidityOutOfRange <= 0.5000000000f) {
      if (f.tempOutOfRange <= 0.5000000000f) {
        if (f.absPressureDelta <= 0.0000276153f) {
          if (f.temperature <= 34.4054241180f) {
            if (f.humidity <= 48.9061737061f) {
              return 0.0135995370f;
            } else {
              return 0.0007676070f;
            }
          } else {
            if (f.absHumidityDelta <= 0.0000004731f) {
              return 0.0400000000f;
            } else {
              return 0.0049751244f;
            }
          }
        } else {
          if (f.pressure <= 1016.1564941406f) {
            if (f.absHumidityDelta <= 21.1140317917f) {
              return 0.0000000000f;
            } else {
              return 0.0025940337f;
            }
          } else {
            return 0.0008022463f;
          }
        }
      } else {
        return 0.0006190034f;
      }
    } else {
      return 0.0005992509f;
    }
  }
}

float std5(int channel) {
  if (historyCount < 2) return 0.0f;
  float mean = 0.0f;
  for (int i = 0; i < historyCount; ++i) {
    mean += (channel == 0 ? history[i].temperature : channel == 1 ? history[i].pressure : history[i].humidity);
  }
  mean /= historyCount;
  float ss = 0.0f;
  for (int i = 0; i < historyCount; ++i) {
    float v = (channel == 0 ? history[i].temperature : channel == 1 ? history[i].pressure : history[i].humidity);
    ss += (v - mean) * (v - mean);
  }
  return sqrtf(ss / historyCount);
}

bool hardAnomaly(const Reading &r, const Reading *prev, int prevCount, String &type, String &reason) {
  if (!finiteReading(r)) {
    type = "INVALID_VALUE";
    reason = "Non-finite sensor value";
    return true;
  }
  if (r.temperature < TEMP_MIN || r.temperature > TEMP_MAX ||
      r.pressure < PRESS_MIN || r.pressure > PRESS_MAX ||
      r.humidity < RH_MIN || r.humidity > RH_MAX) {
    type = "RANGE_VIOLATION";
    reason = "Sensor value outside local operating envelope";
    return true;
  }
  if (prevCount > 0) {
    float dt = fabs(r.temperature - prev[prevCount - 1].temperature);
    float dp = fabs(r.pressure - prev[prevCount - 1].pressure);
    float dh = fabs(r.humidity - prev[prevCount - 1].humidity);
    if (dt >= TEMP_JUMP || dp >= PRESS_JUMP || dh >= RH_JUMP) {
      type = "ABRUPT_SPIKE";
      reason = "Abrupt local change exceeds edge hard-fault gate";
      return true;
    }
  }
  if (prevCount >= WINDOW) {
    bool tFlat = true, pFlat = true, hFlat = true;
    for (int i = 1; i < WINDOW; ++i) {
      tFlat &= fabs(prev[i].temperature - prev[0].temperature) < 1e-6f;
      pFlat &= fabs(prev[i].pressure - prev[0].pressure) < 1e-6f;
      hFlat &= fabs(prev[i].humidity - prev[0].humidity) < 1e-6f;
    }
    if (tFlat || pFlat || hFlat) {
      type = "FROZEN_SENSOR";
      reason = "A sensor channel is unchanged across local rolling window";
      return true;
    }
  }

  EdgeFeatures f{};
  f.temperature = r.temperature; f.pressure = r.pressure; f.humidity = r.humidity;
  f.absTempDelta = prevCount ? fabs(r.temperature - prev[prevCount-1].temperature) : 0.0f;
  f.absPressureDelta = prevCount ? fabs(r.pressure - prev[prevCount-1].pressure) : 0.0f;
  f.absHumidityDelta = prevCount ? fabs(r.humidity - prev[prevCount-1].humidity) : 0.0f;
  f.tempStd5 = std5(0); f.pressureStd5 = std5(1); f.humidityStd5 = std5(2);
  f.tempOutOfRange = (r.temperature < TEMP_MIN || r.temperature > TEMP_MAX);
  f.pressureOutOfRange = (r.pressure < PRESS_MIN || r.pressure > PRESS_MAX);
  f.humidityOutOfRange = (r.humidity < RH_MIN || r.humidity > RH_MAX);
  float pHard = portableTreeHardProbability(f);
  if (pHard >= 0.50f) {
    type = "EDGE_ML_HARD_ANOMALY";
    reason = "Compact portable decision tree classified the observation as a hard anomaly";
    return true;
  }
  return false;
}

void bufferTrusted(const Reading &r) {
  // Replace with your RAM/flash ring buffer implementation.
  if (historyCount < WINDOW) history[historyCount++] = r;
  else {
    for (int i = 1; i < WINDOW; ++i) history[i - 1] = history[i];
    history[WINDOW - 1] = r;
  }
}

void setup() {
  Serial.begin(115200);
  Serial.println("SkyGuard Edge AI booted");
}

void loop() {
  Reading r = {/* replace with sensor reads */ 30.0f, 1012.0f, 75.0f};
  String type, reason;
  bool blocked = hardAnomaly(r, history, historyCount, type, reason);

  if (blocked) {
    Serial.printf("EDGE BLOCKED: %s | %s\n", type.c_str(), reason.c_str());
    // TODO: transmit a compact /api/edge-event message, not the telemetry batch.
  } else {
    bufferTrusted(r);
    Serial.println("EDGE PASS: buffered trusted reading");
  }

  // TODO: flush buffered readings to /api/edge-batch every 5/10 minutes.
  delay(1000);
}
