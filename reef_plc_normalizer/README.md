# Reef PLC Normalizer

Home Assistant App that normalizes AutomationDirect P1-550 packed MQTT payloads
and exposes the PLC clock for reef tank monitoring.

The app subscribes to the raw PLC topics, validates its embedded field layout
against a sanitized PLC-generated MQTT contract, validates packed CSV and ISO
8601 clock payloads, publishes normalized JSON state topics, and publishes
retained Home Assistant MQTT device discovery. The normalized clock state includes both
the PLC timestamp and a signed receipt-time offset in seconds; positive means
the PLC clock is behind the normalizer host and negative means it is ahead.

## MQTT Flow

- Input: `plc/aquarium/di`, `plc/aquarium/do`, `plc/aquarium/ai`,
  `plc/aquarium/inputs`, `plc/aquarium/alarms`, `plc/aquarium/ato`,
  `plc/aquarium/time_sync`, `plc/aquarium/gmp40_1`, `plc/aquarium/clock`
- Cabinet-light command: `plc/aquarium/command/cabinet_light`
- GMP40 commands: `reef/plc/command/gmp40_1/{power,mode,flow,frequency,feed_time}`
- State output: `reef/plc/state/{di,do,ai,inputs,alarms,ato,time_sync,gmp40_1,clock}`
- Availability: data entities require both `plc/aquarium/status` and
  `reef/plc/status`; the PLC MQTT connectivity entity requires only the
  normalizer status
- Discovery:
  `homeassistant/{sensor,binary_sensor,light,switch,select,number}/reef_plc_<entity>/config`

The app also publishes a diagnostic PLC MQTT connectivity binary sensor from the
PLC's retained status/LWT topic, plus topic-health binary sensors for each
normalized state topic. Every data entity and topic-health entity uses Home
Assistant MQTT `expire_after` to become unavailable when its source topic stops
updating. The clock allows 390 seconds for a five-minute publish interval; the
other topics allow 60 seconds.

The eighth digital-output field, `DO_Relay_DC_4`, is exposed as the controllable
Home Assistant light `light.office_reef_cabinet`. Home Assistant sends
non-retained `ON` and `OFF` commands directly to the PLC and optimistically
reflects the requested state immediately. The next normalized digital-output
payload reconciles the light with the confirmed PLC state. Other relay outputs
remain observe-only binary sensors.

GMP40 controls are non-optimistic: their state always comes from the PLC's
confirmed status. The app validates each requested value, emits a seven-byte
one-field masked command to `plc/aquarium/command/gmp40_1`, and waits for the
PLC receive counter before sending the next queued request. Later requests for
the same field are coalesced, and pending requests are discarded on reconnect.

## Configuration

Configure the app with the MQTT broker connection details. The PLC field map and
per-field Home Assistant discovery metadata are defined in
`app/packed_mqtt_layout.yaml` and embedded into the app at build time, so a PLC
pack-string order or entity metadata change should be shipped as a new app
version.

At startup, the app also validates that its vendored contract agrees with the
embedded layout and expected MQTT semantics: PLC status/LWT is retained at QoS
1, telemetry publishers are non-retained, command subscriptions use QoS 1, and
the raw topic field order, type, and width match. Contract drift stops startup
with a precise error instead of silently mislabeling PLC values.

The PLC's unconditional telemetry publishers are the freshness signal; a
separate heartbeat topic is intentionally unnecessary. The retained PLC status
topic provides immediate client availability, while each data topic's
`expire_after` detects a partial publisher failure.

## Installation Notes

Add the standalone `reef-plc-normalizer` repository to Home Assistant as a custom
App repository, or copy this app folder to `/addons/reef_plc_normalizer` on the
Home Assistant OS VM for local installation.
