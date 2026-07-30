# Reef PLC Normalizer

Home Assistant App that normalizes AutomationDirect P1-550 packed MQTT payloads
and exposes the PLC clock for reef tank monitoring.

The app subscribes to the raw PLC topics, validates packed CSV and ISO 8601
clock payloads, publishes normalized JSON state topics, and publishes retained
Home Assistant MQTT device discovery. The normalized clock state includes both
the PLC timestamp and a signed receipt-time offset in seconds; positive means
the PLC clock is behind the normalizer host and negative means it is ahead.

## MQTT Flow

- Input: `plc/aquarium/di`, `plc/aquarium/do`, `plc/aquarium/ai`,
  `plc/aquarium/inputs`, `plc/aquarium/alarms`, `plc/aquarium/ato`,
  `plc/aquarium/time_sync`, `plc/aquarium/clock`
- Cabinet-light command: `plc/aquarium/command/cabinet_light`
- State output: `reef/plc/state/{di,do,ai,inputs,alarms,ato,time_sync,clock}`
- Availability: `reef/plc/status`; the cabinet light also requires
  `plc/aquarium/status`
- Discovery:
  `homeassistant/{sensor,binary_sensor,light}/reef_plc_<entity>/config`

The app also publishes diagnostic topic-health binary sensors for each normalized
state topic. They use Home Assistant MQTT `expire_after` to become unavailable
when a PLC topic stops updating. The clock allows 390 seconds for a five-minute
publish interval; the other topics allow 60 seconds.

The eighth digital-output field, `DO_Relay_DC_4`, is exposed as the controllable
Home Assistant light `light.office_reef_cabinet`. Home Assistant sends
non-retained `ON` and `OFF` commands directly to the PLC and optimistically
reflects the requested state immediately. The next normalized digital-output
payload reconciles the light with the confirmed PLC state. Other relay outputs
remain observe-only binary sensors.

## Configuration

Configure the app with the MQTT broker connection details. The PLC field map and
per-field Home Assistant discovery metadata are defined in
`app/packed_mqtt_layout.yaml` and embedded into the app at build time, so a PLC
pack-string order or entity metadata change should be shipped as a new app
version.

## Installation Notes

Add the standalone `reef-plc-normalizer` repository to Home Assistant as a custom
App repository, or copy this app folder to `/addons/reef_plc_normalizer` on the
Home Assistant OS VM for local installation.
