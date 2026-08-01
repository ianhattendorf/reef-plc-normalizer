# Changelog

## Unreleased

- Install the pinned Rust formatting and lint components explicitly in CI.

## 0.1.18

- Update the container builder and CI to Rust 1.97.1 and declare Rust 1.97 as
  the minimum supported toolchain.

## 0.1.17

- Validate the embedded field layout against a sanitized MQTT contract
  generated from the PLC source, including topics, QoS, retention, timing,
  commands, and packed field metadata.
- Split runtime responsibilities into focused configuration, contract, layout,
  MQTT, normalization, availability, and discovery modules.
- Add a contract synchronization helper and enforce formatting, Clippy, tests,
  release metadata, and the container build in CI.
- Record authoritative PLC source tags separately where stable Home Assistant
  keys intentionally differ.

## 0.1.16

- Require both the PLC and normalizer MQTT status topics for every discovered
  data and topic-health entity.
- Apply per-source freshness expiry to all discovered sensors and binary
  sensors, using 60 seconds for telemetry and 390 seconds for the clock.
- Add a diagnostic PLC MQTT connectivity entity backed directly by the PLC's
  retained status and last-will topic.

## 0.1.15

- Add cabinet-light request and open-door on-time alarms to the packed alarm
  topic.

## 0.1.14

- Make the cabinet light optimistic so commands appear immediately while the
  next PLC digital-output state reconciles the result.

## 0.1.13

- Expose the eighth PLC digital output as a confirmed-state MQTT cabinet light
  with direct PLC commands and combined PLC/normalizer availability.
- Remove the former retained DC Relay 4 binary-sensor discovery record.

## 0.1.12

- Allow 390 seconds before marking the five-minute PLC clock topic stale.
- Publish the PLC clock's signed receipt-time offset for reliable Home Assistant alerts.

## 0.1.11

- Add the PLC clock as a Home Assistant timestamp sensor.

## 0.1.10

- Add PLC SNTP time-sync alarm, battery, and counter entities.
- Preserve locked dependency versions when preparing releases.

## 0.1.9

- Increase sump temperature precision to two decimal places.

## 0.1.8

- Add the not-auto-mode time alarm to the packed alarm topic.

## 0.1.7

- Harden MQTT recovery after broker restarts and transient connection loss.

## 0.1.6

- Add the return float low-time alarm to the packed alarm topic.

## 0.1.5

- Add diagnostic MQTT topic-health entities with 60-second freshness expiry.

## 0.1.4

- Add current and accumulated ATO milliliter sensors to the packed ATO topic.

## 0.1.3

- Add heater on-time and pH alarm entities to the packed alarm topic.

## 0.1.2

- Add packed alarm topic normalization and Home Assistant discovery entities.

## 0.1.1

- Add release metadata updates for Home Assistant update detection.

## 0.1.0

- Initial Reef PLC MQTT normalizer app.
