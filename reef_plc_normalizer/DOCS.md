# Reef PLC Normalizer

This app turns fixed-length packed MQTT strings and the clock timestamp from the
AutomationDirect P1-550 reef PLC into Home Assistant MQTT entities.

The normalized clock payload preserves `PLC_Clock` and adds
`Clock_Offset_Seconds`, calculated when the raw PLC message is received:

`normalizer receipt time - PLC clock time`

A positive offset means the PLC clock is behind; a negative offset means it is
ahead. Home Assistant should use this numeric entity for clock-offset alerts
instead of deriving receipt time from entity state metadata.

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
values. The PLC clock must use `YYYY-MM-DDTHH:MM:SS±HH:MM` ISO 8601 format.
Rejected payloads are logged and do not update Home Assistant state.

## Cabinet Light Control

The eighth field in `plc/aquarium/do`, `DO_Relay_DC_4`, is discovered as
`light.office_reef_cabinet`. Its MQTT flow is:

- Home Assistant publishes `ON` or `OFF` to
  `plc/aquarium/command/cabinet_light` with QoS 1 and retain disabled.
- The PLC applies the request in ladder logic and publishes the effective
  physical output in `plc/aquarium/do`.
- Home Assistant optimistically reflects the requested state immediately.
- The app publishes the normalized output in `reef/plc/state/do`; the next
  confirmed PLC state reconciles the Home Assistant light.

The light is optimistic and is available only when both retained status topics
contain `online`:

- `plc/aquarium/status`, owned by the PLC.
- `reef/plc/status`, owned by this app.

The PLC status topic should use retained `online` state and a retained `offline`
last will. The app does not merge or republish PLC availability under its own
status topic.

The app also discovers `binary_sensor.reef_plc_mqtt_connected` directly from
the PLC status topic. It is available whenever the normalizer is connected, and
its `on`/`off` state reports the PLC MQTT client's retained online state or
retained offline last will.

On startup and reconnect, the app removes the former retained
`homeassistant/binary_sensor/reef_plc_do_relay_dc_4/config` discovery record
before publishing the cabinet-light discovery record.

## Topic Health

The app publishes diagnostic MQTT binary sensor discovery for each normalized
state topic: DI, DO, AI, inputs, alarms, ATO, time sync, and clock. These
entities use the normalized state topic as their `state_topic` and always
render the latest payload as `ON`. Every sensor, binary sensor, clock-offset
sensor, and topic-health entity sourced from those topics uses the same expiry:
most set `expire_after: 60`; the clock uses `expire_after: 390` to allow a
five-minute publish interval plus 90 seconds of delivery and reconnect
tolerance. If one PLC topic stops producing fresh payloads while both MQTT
clients stay online, only entities sourced from that topic become unavailable.

Data and topic-health entities require both retained availability topics in
`all` mode: `plc/aquarium/status` for the PLC MQTT client and `reef/plc/status`
for the normalizer. `expire_after` independently tracks per-topic freshness.
This makes STOP mode uniform: the PLC LWT makes data unavailable immediately,
while a failure of only one publisher appears as that topic's expiry after its
grace period.

## MQTT Recovery

The app keeps polling the MQTT event loop after transient connection failures.
After each successful connection or reconnection, it republishes its retained
availability payload, resubscribes to PLC and Home Assistant status topics, and
republishes retained Home Assistant discovery. Recent normalized states are
replayed only when they were received within the last 60 seconds. This
conservative replay window remains shorter than the clock topic's 390-second
expiry so a broker reconnect does not replay an older clock reading as newly
received.

## Packed MQTT Layout

The packed MQTT topic layout and per-field Home Assistant discovery metadata are
defined in `app/packed_mqtt_layout.yaml`. Each field records the PLC source tag,
packed character length, value type, and Home Assistant discovery metadata. The
PLC source tag is also used as the normalized JSON state key. The file is
embedded at build time and is not exposed as a Home Assistant option.

Boolean fields can set `active_when: false` for normally-closed inputs where raw
`0` means active. Individual entities can set
`discovery.enabled_by_default: false` to publish Home Assistant discovery while
leaving the entity disabled by default. A discovery entry can override its
source-derived ID with `component_id`, seed its first Home Assistant entity ID
with `default_entity_id`, and use the `light` domain with a required
`command_topic`. Removed retained discovery records are listed explicitly under
the top-level `removed_discovery` key.
