# Reef PLC Normalizer

Home Assistant App that normalizes AutomationDirect P1-550 packed MQTT payloads
for reef tank monitoring.

The app subscribes to the raw PLC topics, validates the packed CSV field counts,
publishes normalized JSON state topics, and publishes retained Home Assistant
MQTT device discovery.

## MQTT Flow

- Input: `plc/aquarium/di`, `plc/aquarium/do`, `plc/aquarium/ai`,
  `plc/aquarium/inputs`, `plc/aquarium/alarms`, `plc/aquarium/ato`
- State output: `reef/plc/state/{di,do,ai,inputs,alarms,ato}`
- Availability: `reef/plc/status`
- Discovery: `homeassistant/{sensor,binary_sensor}/reef_plc_<entity>/config`

Home Assistant remains observe-only. Relay outputs are exposed as binary sensors,
not switches.

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
