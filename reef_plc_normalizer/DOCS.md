# Reef PLC Normalizer

This app turns fixed-length packed MQTT strings from the AutomationDirect P1-550
reef PLC into Home Assistant MQTT entities.

## Options

- `mqtt_host`: MQTT broker host.
- `mqtt_port`: MQTT broker port.
- `mqtt_username`: MQTT username. Leave empty for anonymous brokers.
- `mqtt_password`: MQTT password.
- `discovery_prefix`: Home Assistant MQTT discovery prefix.
- `publish_diagnostic_ai`: Publish raw analog input registers as diagnostic
  sensors. Disabled by default.
- `log_level`: Rust tracing log level.

## Validation

Each PLC topic has a fixed field count. The app trims whitespace, accepts one
trailing comma, and rejects payloads with the wrong number of fields or invalid
values. Rejected payloads are logged and do not update Home Assistant state.

## Packed MQTT Layout

The packed MQTT topic layout and per-field Home Assistant discovery metadata are
defined in `app/packed_mqtt_layout.yaml`. Each field records the PLC source tag,
packed character length, value type, and Home Assistant discovery metadata. The
PLC source tag is also used as the normalized JSON state key. The file is
embedded at build time and is not exposed as a Home Assistant option.

Boolean fields can set `active_when: false` for normally-closed inputs where raw
`0` means active. Individual entities can set
`discovery.enabled_by_default: false` to publish Home Assistant discovery while
leaving the entity disabled by default.
