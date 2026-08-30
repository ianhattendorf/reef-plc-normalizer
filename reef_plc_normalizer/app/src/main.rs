use anyhow::Result;
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod availability;
mod command;
mod config;
mod contract;
mod discovery;
mod layout;
mod mqtt;
mod normalize;

use config::{load_options, Args};
use layout::load_layout;
use mqtt::run;

#[cfg(test)]
use chrono::DateTime;
#[cfg(test)]
use {
    availability::{fresh_cached_states, CachedState, ReconnectBackoff},
    config::AppOptions,
    discovery::{discovery_messages, removed_discovery_topics},
    layout::{validate_layout, Domain, Layout, TopicKind, ValueType},
    normalize::{normalize_payload, parse_payload, ParsePayloadError, CLOCK_OFFSET_FIELD},
    serde_json::{json, Map, Value},
    std::collections::HashMap,
    std::time::{Duration, Instant, UNIX_EPOCH},
};

const APP_NAME: &str = "reef-plc-normalizer";
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const DEVICE_ID: &str = "reef_plc";
const DEVICE_NAME: &str = "Reef PLC";
const AVAILABILITY_TOPIC: &str = "reef/plc/status";
const PLC_AVAILABILITY_TOPIC: &str = "plc/aquarium/status";
const DEFAULT_TOPIC_HEALTH_EXPIRE_AFTER_SECONDS: u64 = 60;
const CLOCK_TOPIC_HEALTH_EXPIRE_AFTER_SECONDS: u64 = 390;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let options = load_options(&args.options)?;
    let layout = load_layout()?;
    init_logging(&options.log_level)?;

    info!(
        mqtt_host = %options.mqtt_host,
        mqtt_port = options.mqtt_port,
        discovery_prefix = %options.discovery_prefix,
        publish_diagnostic_ai = options.publish_diagnostic_ai,
        "starting Reef PLC normalizer"
    );

    run(options, layout).await
}

fn init_logging(level: &str) -> Result<()> {
    let filter = EnvFilter::try_new(level).or_else(|_| EnvFilter::try_new("info"))?;
    tracing_subscriber::fmt().with_env_filter(filter).init();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn embedded_layout_loads_and_validates() {
        let layout = test_layout();

        assert_eq!(layout.topics.len(), 9);
        assert!(layout
            .topics
            .iter()
            .any(|spec| spec.source_topic == "plc/aquarium/inputs"));
        assert!(layout
            .topics
            .iter()
            .any(|spec| spec.source_topic == "plc/aquarium/alarms"));
        assert!(layout
            .topics
            .iter()
            .any(|spec| spec.source_topic == "plc/aquarium/time_sync"));
        assert!(layout
            .topics
            .iter()
            .any(|spec| spec.source_topic == "plc/aquarium/clock"));

        let di = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Di)
            .unwrap();
        assert_eq!(di.fields[4].source, "DI_Return_Float_LowLow");
        assert_eq!(di.fields[4].length, 1);

        let digital_outputs = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Do)
            .unwrap();
        assert_eq!(digital_outputs.fields[7].source, "DO_Relay_DC_4");
        assert_eq!(digital_outputs.fields[7].discovery.domain, Domain::Light);
        assert_eq!(
            digital_outputs.fields[7].discovery.command_topic.as_deref(),
            Some("plc/aquarium/command/cabinet_light")
        );

        let inputs = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Inputs)
            .unwrap();
        assert_eq!(inputs.fields[13].source, "ATO_Amps");
        assert_eq!(inputs.fields[13].length, 4);
        assert_eq!(inputs.fields[15].source, "Ph_Transmitter");
        assert_eq!(inputs.fields[15].length, 4);

        let alarms = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Alarms)
            .unwrap();
        assert_eq!(alarms.fields.len(), 23);
        assert_eq!(alarms.fields[0].source, "Alarm_Heater_Not_On");
        assert_eq!(alarms.fields[11].source, "Alarm_ATO_Runtime");
        assert_eq!(alarms.fields[12].source, "Alarm_Heater_1_On_Time");
        assert_eq!(alarms.fields[13].source, "Alarm_Ph");
        assert_eq!(alarms.fields[14].source, "Alarm_Return_Float_Low_Time");
        assert_eq!(alarms.fields[15].source, "Alarm_Not_Auto_Mode_Time");
        assert_eq!(alarms.fields[16].source, "Alarm_Cab_Light_Req_On_Time");
        assert_eq!(alarms.fields[17].source, "Alarm_Cab_Light_Door_On_Time");
        assert_eq!(alarms.fields[18].source, "Alarm_GMP40_1_Any");
        assert_eq!(alarms.fields[19].source, "Alarm_GMP40_1_Command_Failed");
        assert_eq!(alarms.fields[20].source, "Alarm_GMP40_1_Device_Fault");
        assert_eq!(alarms.fields[21].source, "Alarm_GMP40_1_Protocol_Fault");
        assert_eq!(alarms.fields[22].source, "Alarm_GMP40_1_Unavailable");

        let ato = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Ato)
            .unwrap();
        assert_eq!(ato.fields.len(), 9);
        assert_eq!(ato.fields[7].source, "ATO_Current_mL");
        assert_eq!(ato.fields[7].length, 4);
        assert_eq!(ato.fields[8].source, "ATO_Acc_mL");
        assert_eq!(ato.fields[8].length, 4);

        let time_sync = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::TimeSync)
            .unwrap();
        assert_eq!(time_sync.fields.len(), 4);
        assert_eq!(time_sync.fields[0].source, "Alarm_Time_Sync");
        assert_eq!(time_sync.fields[0].length, 1);
        assert_eq!(time_sync.fields[1].source, "Battery Low Bit");
        assert_eq!(time_sync.fields[1].length, 1);
        assert_eq!(time_sync.fields[2].source, "Time_Sync_Error_Count.Counter");
        assert_eq!(time_sync.fields[2].length, 5);
        assert_eq!(
            time_sync.fields[3].source,
            "Time_Sync_Success_Count.Counter"
        );
        assert_eq!(time_sync.fields[3].length, 5);

        let clock = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Clock)
            .unwrap();
        assert_eq!(clock.fields.len(), 1);
        assert_eq!(clock.fields[0].source, "PLC_Clock");
        assert_eq!(clock.fields[0].length, 25);
        assert_eq!(clock.fields[0].value_type, ValueType::Timestamp);
    }

    #[test]
    fn parses_alarm_payloads() {
        let layout = test_layout();
        let spec = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Alarms)
            .unwrap();
        let state = parse_payload(spec, "1,0,0,1,0,1,0,0,1,0,1,0,1,0,1,0,1,0,1,0,1,0,1,").unwrap();

        assert_eq!(state["Alarm_Heater_Not_On"], json!(true));
        assert_eq!(state["Alarm_Heater_On"], json!(false));
        assert_eq!(state["Alarm_Return_Pump_Not_Running"], json!(true));
        assert_eq!(state["Alarm_ATO_Runtime"], json!(false));
        assert_eq!(state["Alarm_Heater_1_On_Time"], json!(true));
        assert_eq!(state["Alarm_Ph"], json!(false));
        assert_eq!(state["Alarm_Return_Float_Low_Time"], json!(true));
        assert_eq!(state["Alarm_Not_Auto_Mode_Time"], json!(false));
        assert_eq!(state["Alarm_Cab_Light_Req_On_Time"], json!(true));
        assert_eq!(state["Alarm_Cab_Light_Door_On_Time"], json!(false));
        assert_eq!(state["Alarm_GMP40_1_Any"], json!(true));
        assert_eq!(state["Alarm_GMP40_1_Command_Failed"], json!(false));
        assert_eq!(state["Alarm_GMP40_1_Device_Fault"], json!(true));
        assert_eq!(state["Alarm_GMP40_1_Protocol_Fault"], json!(false));
        assert_eq!(state["Alarm_GMP40_1_Unavailable"], json!(true));
    }

    #[test]
    fn parses_ato_volume_payloads() {
        let layout = test_layout();
        let spec = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Ato)
            .unwrap();
        let state = parse_payload(spec, "12,1,345,678,0,1,0,42,1234,").unwrap();

        assert_eq!(state["ATO_Timer.Current"], json!(12));
        assert_eq!(state["ATO_Timer.Done"], json!(true));
        assert_eq!(state["ATO_Current_mL"], json!(42));
        assert_eq!(state["ATO_Acc_mL"], json!(1234));
    }

    #[test]
    fn parses_gmp40_telemetry() {
        let layout = test_layout();
        let spec = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Gmp40)
            .unwrap();
        let state = parse_payload(
            spec,
            "1,1,1,3,048,028,037,0A,000,1,1,20,0,0,1,0,2,0,7,42,3,",
        )
        .unwrap();

        assert_eq!(state["GMP40_1_Data.Status.Power"], json!(true));
        assert_eq!(state["GMP40_1_Data.Status.Mode"], json!(3));
        assert_eq!(state["GMP40_1_Data.Status.Flow"], json!(72));
        assert_eq!(state["GMP40_1_Authority.MQTTReceivedLast"], json!(42));
        assert_eq!(state["GMP40_1_Data.Status.Linkage"], json!(3));
    }

    #[test]
    fn parses_time_sync_payloads() {
        let layout = test_layout();
        let spec = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::TimeSync)
            .unwrap();
        let state = parse_payload(spec, "1,0,00012,00345,").unwrap();

        assert_eq!(state["Alarm_Time_Sync"], json!(true));
        assert_eq!(state["Battery Low Bit"], json!(false));
        assert_eq!(state["Time_Sync_Error_Count.Counter"], json!(12));
        assert_eq!(state["Time_Sync_Success_Count.Counter"], json!(345));
    }

    #[test]
    fn parses_clock_payloads() {
        let layout = test_layout();
        let spec = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Clock)
            .unwrap();
        let state = parse_payload(spec, "2026-07-26T18:11:10-07:00").unwrap();

        assert_eq!(state["PLC_Clock"], json!("2026-07-26T18:11:10-07:00"));
    }

    #[test]
    fn normalizes_clock_with_receipt_derived_offset() {
        let layout = test_layout();
        let spec = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Clock)
            .unwrap();
        let plc_timestamp = DateTime::parse_from_rfc3339("2026-07-26T18:11:10-07:00").unwrap();
        let received_at =
            UNIX_EPOCH + Duration::from_millis((plc_timestamp.timestamp_millis() + 2_500) as u64);

        let state = normalize_payload(spec, "2026-07-26T18:11:10-07:00", received_at).unwrap();

        assert_eq!(state["PLC_Clock"], json!("2026-07-26T18:11:10-07:00"));
        assert_eq!(state[CLOCK_OFFSET_FIELD], json!(2.5));
    }

    #[test]
    fn rejects_invalid_clock_payloads() {
        let layout = test_layout();
        let spec = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Clock)
            .unwrap();

        for payload in [
            "2026-02-29T18:11:10-07:00",
            "2026-07-26 18:11:10-07:00",
            "2026-07-26T25:11:10-07:00",
            "not-a-timestamp",
        ] {
            let err = parse_payload(spec, payload).unwrap_err();
            assert!(matches!(err, ParsePayloadError::InvalidTimestamp { .. }));
        }
    }

    #[test]
    fn parses_inputs_with_trailing_comma() {
        let layout = test_layout();
        let spec = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Inputs)
            .unwrap();
        let state = parse_payload(
            spec,
            "78.34,78.12,78.34,0.57,0.40,0.00,0.41,0.35,0.12,0.10,0,1,1,0.05,1,8.12,",
        )
        .unwrap();

        assert_eq!(state["Temp_Sump_1"], json!(78.34));
        assert_eq!(state["Temp_Sump_2"], json!(78.12));
        assert_eq!(state["Temp_Sump_Max"], json!(78.34));
        assert_eq!(state["Ph_Transmitter"], json!(8.12));
        assert_eq!(state["Heater_2_Amps"], json!(0.0));
        assert_eq!(state["Wavemakers_Amps"], json!(0.10));
        assert_eq!(state["ATO_Amps"], json!(0.05));
        assert_eq!(state["ATO_Running"], json!(true));
    }

    #[test]
    fn normalizes_bool_polarity_from_layout() {
        let layout = test_layout();
        let spec = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Di)
            .unwrap();
        let state = parse_payload(spec, "1,0,1,0,0,0,0,0,0,0,0,0,1,0,0,1").unwrap();

        assert_eq!(state["DI_Water_Leak_1"], json!(false));
        assert_eq!(state["DI_Water_Leak_2"], json!(true));
        assert_eq!(state["DI_Return_Float_High"], json!(true));
    }

    #[test]
    fn trims_padded_ai_values() {
        let layout = test_layout();
        let spec = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Ai)
            .unwrap();
        let state = parse_payload(
            spec,
            "313 ,1   ,223 ,1   ,2   ,0   ,45  ,1   ,11  ,1   ,577 ,1   ,9,0,8,1",
        )
        .unwrap();

        assert_eq!(state["AI_CT_AC_Total"], json!(313));
        assert_eq!(state["AI_CT_DC_Wavemakers:1"], json!(1));
    }

    #[test]
    fn rejects_short_payloads() {
        let layout = test_layout();
        let spec = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Inputs)
            .unwrap();
        let err = parse_payload(spec, "78.3,78.1,78.3").unwrap_err();

        assert!(matches!(
            err,
            ParsePayloadError::CountMismatch {
                expected: 16,
                actual: 3,
                ..
            }
        ));
    }

    #[test]
    fn rejects_invalid_bool_values() {
        let layout = test_layout();
        let spec = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Di)
            .unwrap();
        let err = parse_payload(spec, "1,0,wat,0,0,0,0,0,0,0,0,0,1,0,0,1").unwrap_err();

        assert!(matches!(
            err,
            ParsePayloadError::InvalidBool {
                ref field,
                ..
            } if field == "DI_Return_Float_High"
        ));
    }

    #[test]
    fn discovery_omits_ai_by_default() {
        let layout = test_layout();
        let options = test_options(false);
        let components = discovery_components(&options, &layout);

        assert!(components.contains_key("total_amps"));
        assert!(components.contains_key("di_water_leak_1"));
        assert!(components.contains_key("ato_timer_current"));
        assert!(components.contains_key("ai_topic_online"));
        assert!(!components.contains_key("ai_ct_ac_total"));
    }

    #[test]
    fn discovery_suggests_two_decimal_places_for_sump_temperatures() {
        let layout = test_layout();
        let options = test_options(false);
        let components = discovery_components(&options, &layout);

        for component_id in ["temp_sump_1", "temp_sump_2", "temp_sump_max"] {
            assert_eq!(
                components[component_id]["suggested_display_precision"],
                json!(2)
            );
        }
    }

    #[test]
    fn discovery_includes_time_sync_diagnostics() {
        let layout = test_layout();
        let options = test_options(false);
        let components = discovery_components(&options, &layout);

        assert_eq!(
            components["alarm_time_sync"]["state_topic"],
            json!("reef/plc/state/time_sync")
        );
        assert_eq!(
            components["alarm_time_sync"]["value_template"],
            json!("{{ 'ON' if value_json[\"Alarm_Time_Sync\"] else 'OFF' }}")
        );
        assert_eq!(
            components["alarm_time_sync"]["device_class"],
            json!("problem")
        );
        assert_eq!(
            components["battery_low_bit"]["value_template"],
            json!("{{ 'ON' if value_json[\"Battery Low Bit\"] else 'OFF' }}")
        );
        for component_id in [
            "time_sync_error_count_counter",
            "time_sync_success_count_counter",
        ] {
            assert_eq!(
                components[component_id]["state_class"],
                json!("total_increasing")
            );
            assert_eq!(
                components[component_id]["entity_category"],
                json!("diagnostic")
            );
        }
    }

    #[test]
    fn discovery_includes_cabinet_light_on_time_alarms() {
        let layout = test_layout();
        let options = test_options(false);
        let components = discovery_components(&options, &layout);

        for (component_id, source) in [
            ("alarm_cab_light_req_on_time", "Alarm_Cab_Light_Req_On_Time"),
            (
                "alarm_cab_light_door_on_time",
                "Alarm_Cab_Light_Door_On_Time",
            ),
        ] {
            assert_eq!(
                components[component_id]["state_topic"],
                json!("reef/plc/state/alarms")
            );
            assert_eq!(
                components[component_id]["value_template"],
                json!(format!(
                    "{{{{ 'ON' if value_json[\"{source}\"] else 'OFF' }}}}"
                ))
            );
            assert_eq!(components[component_id]["device_class"], json!("problem"));
        }
    }

    #[test]
    fn discovery_includes_gmp40_alarms() {
        let layout = test_layout();
        let options = test_options(false);
        let components = discovery_components(&options, &layout);

        for (component_id, source) in [
            ("alarm_gmp40_1_any", "Alarm_GMP40_1_Any"),
            (
                "alarm_gmp40_1_command_failed",
                "Alarm_GMP40_1_Command_Failed",
            ),
            ("alarm_gmp40_1_device_fault", "Alarm_GMP40_1_Device_Fault"),
            (
                "alarm_gmp40_1_protocol_fault",
                "Alarm_GMP40_1_Protocol_Fault",
            ),
            ("alarm_gmp40_1_unavailable", "Alarm_GMP40_1_Unavailable"),
        ] {
            assert_eq!(
                components[component_id]["state_topic"],
                json!("reef/plc/state/alarms")
            );
            assert_eq!(
                components[component_id]["value_template"],
                json!(format!(
                    "{{{{ 'ON' if value_json[\"{source}\"] else 'OFF' }}}}"
                ))
            );
            assert_eq!(components[component_id]["device_class"], json!("problem"));
        }
    }

    #[test]
    fn discovery_includes_confirmed_gmp40_controls() {
        let layout = test_layout();
        let options = test_options(false);
        let messages = discovery_messages(&options, &layout);
        let components = discovery_components(&options, &layout);

        assert!(messages
            .iter()
            .any(|(topic, _)| topic == "homeassistant/switch/reef_plc_gmp40_1_power/config"));
        assert!(messages
            .iter()
            .any(|(topic, _)| topic == "homeassistant/select/reef_plc_gmp40_1_mode/config"));
        assert!(messages
            .iter()
            .any(|(topic, _)| topic == "homeassistant/number/reef_plc_gmp40_1_flow/config"));
        assert_eq!(components["gmp40_1_power"]["optimistic"], json!(false));
        assert_eq!(
            components["gmp40_1_power"]["command_topic"],
            json!("reef/plc/command/gmp40_1/power")
        );
        assert_eq!(
            components["gmp40_1_mode"]["options"],
            json!([
                "pulse_wave",
                "sine_wave",
                "constant_flow",
                "random_wave",
                "tide",
                "nutrient_transport",
                "circulation",
                "feeding",
                "custom_wave"
            ])
        );
        assert_eq!(
            components["gmp40_1_mode"]["value_template"],
            json!("{{ {0: 'pulse_wave', 1: 'sine_wave', 2: 'constant_flow', 3: 'random_wave', 4: 'tide', 5: 'nutrient_transport', 6: 'circulation', 7: 'feeding', 8: 'custom_wave'}.get(value_json[\"GMP40_1_Data.Status.Mode\"] | int) }}")
        );
        assert_eq!(
            components["gmp40_1_linkage"]["value_template"],
            json!("{{ {0: 'independent', 1: 'primary', 2: 'synchronous_secondary', 3: 'asynchronous_secondary'}.get(value_json[\"GMP40_1_Data.Status.Linkage\"] | int) }}")
        );
        assert_eq!(components["gmp40_1_flow"]["min"], json!(0));
        assert_eq!(components["gmp40_1_flow"]["max"], json!(100));
    }

    #[test]
    fn discovery_includes_plc_clock_timestamp() {
        let layout = test_layout();
        let options = test_options(false);
        let components = discovery_components(&options, &layout);

        assert_eq!(
            components["plc_clock"]["state_topic"],
            json!("reef/plc/state/clock")
        );
        assert_eq!(
            components["plc_clock"]["value_template"],
            json!("{{ value_json[\"PLC_Clock\"] }}")
        );
        assert_eq!(components["plc_clock"]["device_class"], json!("timestamp"));
        assert_eq!(
            components["plc_clock"]["entity_category"],
            json!("diagnostic")
        );
    }

    #[test]
    fn discovery_includes_receipt_derived_clock_offset() {
        let layout = test_layout();
        let options = test_options(false);
        let messages = discovery_messages(&options, &layout);
        let components = discovery_components(&options, &layout);

        assert!(
            messages
                .iter()
                .any(|(topic, _)| topic
                    == "homeassistant/sensor/reef_plc_clock_offset_seconds/config")
        );
        assert_eq!(
            components["clock_offset_seconds"]["state_topic"],
            json!("reef/plc/state/clock")
        );
        assert_eq!(
            components["clock_offset_seconds"]["value_template"],
            json!("{{ value_json[\"Clock_Offset_Seconds\"] }}")
        );
        assert_eq!(
            components["clock_offset_seconds"]["unit_of_measurement"],
            json!("s")
        );
        assert_eq!(
            components["clock_offset_seconds"]["state_class"],
            json!("measurement")
        );
        assert_eq!(
            components["clock_offset_seconds"]["suggested_display_precision"],
            json!(1)
        );
        assert_eq!(
            components["clock_offset_seconds"]["entity_category"],
            json!("diagnostic")
        );
        assert_eq!(
            components["clock_offset_seconds"]["expire_after"],
            json!(390)
        );
        assert_eq!(
            components["clock_offset_seconds"]["availability_mode"],
            json!("all")
        );
    }

    #[test]
    fn discovery_includes_plc_mqtt_connectivity() {
        let layout = test_layout();
        let options = test_options(false);
        let messages = discovery_messages(&options, &layout);
        let components = discovery_components(&options, &layout);

        assert!(messages.iter().any(
            |(topic, _)| topic == "homeassistant/binary_sensor/reef_plc_mqtt_connected/config"
        ));
        let connected = &components["mqtt_connected"];
        assert_eq!(connected["state_topic"], json!("plc/aquarium/status"));
        assert_eq!(connected["payload_on"], json!("online"));
        assert_eq!(connected["payload_off"], json!("offline"));
        assert_eq!(connected["availability_topic"], json!("reef/plc/status"));
        assert_eq!(connected["device_class"], json!("connectivity"));
        assert_eq!(connected["entity_category"], json!("diagnostic"));
        assert!(connected.get("expire_after").is_none());
        assert!(connected.get("availability").is_none());
    }

    #[test]
    fn discovery_includes_topic_health_sensors() {
        let layout = test_layout();
        let options = test_options(false);
        let messages = discovery_messages(&options, &layout);
        let components = discovery_components(&options, &layout);

        for (component_id, state_topic) in [
            ("di_topic_online", "reef/plc/state/di"),
            ("do_topic_online", "reef/plc/state/do"),
            ("ai_topic_online", "reef/plc/state/ai"),
            ("inputs_topic_online", "reef/plc/state/inputs"),
            ("alarms_topic_online", "reef/plc/state/alarms"),
            ("ato_topic_online", "reef/plc/state/ato"),
            ("time_sync_topic_online", "reef/plc/state/time_sync"),
            ("clock_topic_online", "reef/plc/state/clock"),
        ] {
            assert!(messages.iter().any(|(topic, _)| topic
                == &format!("homeassistant/binary_sensor/reef_plc_{component_id}/config")));
            assert_eq!(
                components[component_id]["unique_id"],
                json!(format!("reef_plc_{component_id}"))
            );
            assert_eq!(components[component_id]["state_topic"], json!(state_topic));
            assert_eq!(components[component_id]["value_template"], json!("ON"));
            assert_eq!(components[component_id]["payload_on"], json!("ON"));
            let expected_expire_after = if component_id == "clock_topic_online" {
                390
            } else {
                60
            };
            assert_eq!(
                components[component_id]["expire_after"],
                json!(expected_expire_after)
            );
            assert_eq!(components[component_id]["availability_mode"], json!("all"));
            assert_eq!(
                components[component_id]["availability"],
                combined_availability()
            );
            assert!(components[component_id].get("availability_topic").is_none());
            assert_eq!(
                components[component_id]["device_class"],
                json!("connectivity")
            );
            assert_eq!(
                components[component_id]["entity_category"],
                json!("diagnostic")
            );
            assert!(components[component_id].get("force_update").is_none());
        }
    }

    #[test]
    fn discovery_includes_diagnostic_ai_when_enabled() {
        let layout = test_layout();
        let options = test_options(true);
        let components = discovery_components(&options, &layout);

        assert_eq!(
            components["ai_ct_ac_total"]["entity_category"],
            json!("diagnostic")
        );
        assert_eq!(
            components["ai_ct_ac_total"]["enabled_by_default"],
            json!(false)
        );
    }

    #[test]
    fn discovery_can_disable_individual_entities_by_default() {
        let layout = test_layout();
        let options = test_options(false);
        let components = discovery_components(&options, &layout);

        assert_eq!(
            components["di_water_leak_2"]["enabled_by_default"],
            json!(false)
        );
        assert!(components["di_water_leak_1"]
            .as_object()
            .unwrap()
            .get("enabled_by_default")
            .is_none());
        assert!(components["ato_timer_current"]
            .as_object()
            .unwrap()
            .get("enabled_by_default")
            .is_none());
        assert_eq!(
            components["ato_timer_current"]["unit_of_measurement"],
            json!("s")
        );
        assert_eq!(
            components["ato_timer_current"]["device_class"],
            json!("duration")
        );
        assert_eq!(
            components["ato_timer_current"]["state_class"],
            json!("measurement")
        );
    }

    #[test]
    fn discovery_includes_controllable_cabinet_light() {
        let layout = test_layout();
        let options = test_options(false);
        let messages = discovery_messages(&options, &layout);
        let components = discovery_components(&options, &layout);
        let cabinet_light = &components["cabinet_light"];

        assert!(messages
            .iter()
            .any(|(topic, _)| topic == "homeassistant/light/reef_plc_cabinet_light/config"));
        assert_eq!(cabinet_light["unique_id"], json!("reef_plc_cabinet_light"));
        assert_eq!(
            cabinet_light["default_entity_id"],
            json!("light.office_reef_cabinet")
        );
        assert_eq!(
            cabinet_light["command_topic"],
            json!("plc/aquarium/command/cabinet_light")
        );
        assert_eq!(cabinet_light["state_topic"], json!("reef/plc/state/do"));
        assert_eq!(
            cabinet_light["state_value_template"],
            json!("{{ 'ON' if value_json[\"DO_Relay_DC_4\"] else 'OFF' }}")
        );
        assert_eq!(cabinet_light["payload_on"], json!("ON"));
        assert_eq!(cabinet_light["payload_off"], json!("OFF"));
        assert_eq!(cabinet_light["optimistic"], json!(true));
        assert_eq!(cabinet_light["qos"], json!(1));
        assert_eq!(cabinet_light["retain"], json!(false));
        assert_eq!(cabinet_light["availability_mode"], json!("all"));
        assert_eq!(
            cabinet_light["availability"],
            json!([
                {
                    "topic": "plc/aquarium/status",
                    "payload_available": "online",
                    "payload_not_available": "offline"
                },
                {
                    "topic": "reef/plc/status",
                    "payload_available": "online",
                    "payload_not_available": "offline"
                }
            ])
        );
        assert!(cabinet_light.get("availability_topic").is_none());
        assert!(cabinet_light.get("value_template").is_none());
    }

    #[test]
    fn discovery_removes_legacy_cabinet_light_binary_sensor() {
        let layout = test_layout();
        let options = test_options(false);

        assert_eq!(
            removed_discovery_topics(&options, &layout),
            vec!["homeassistant/binary_sensor/reef_plc_do_relay_dc_4/config"]
        );
    }

    #[test]
    fn layout_rejects_light_without_command_topic() {
        let mut layout = test_layout();
        let cabinet_light = layout
            .topics
            .iter_mut()
            .find(|spec| spec.kind == TopicKind::Do)
            .unwrap()
            .fields
            .get_mut(7)
            .unwrap();
        cabinet_light.discovery.command_topic = None;

        let err = validate_layout(&layout).unwrap_err();

        assert!(err
            .to_string()
            .contains("packed MQTT controllable field DO_Relay_DC_4 requires command_topic"));
    }

    #[test]
    fn discovery_uses_per_entity_discovery_shape() {
        let layout = test_layout();
        let options = test_options(false);
        let messages = discovery_messages(&options, &layout);
        let components = discovery_components(&options, &layout);

        assert!(messages
            .iter()
            .any(|(topic, _)| topic == "homeassistant/sensor/reef_plc_total_amps/config"));
        assert!(messages.iter().any(
            |(topic, _)| topic == "homeassistant/binary_sensor/reef_plc_di_water_leak_1/config"
        ));
        assert_eq!(
            components["total_amps"]["device"]["identifiers"],
            json!([DEVICE_ID])
        );
        assert_eq!(components["total_amps"]["origin"]["name"], json!(APP_NAME));
        assert_eq!(components["total_amps"]["unit_of_measurement"], json!("A"));
        assert_eq!(components["total_amps"]["device_class"], json!("current"));
        assert_eq!(
            components["total_amps"]["state_topic"],
            json!("reef/plc/state/inputs")
        );
        assert_eq!(
            components["di_water_leak_1"]["value_template"],
            json!("{{ 'ON' if value_json[\"DI_Water_Leak_1\"] else 'OFF' }}")
        );
        assert_eq!(components["total_amps"]["expire_after"], json!(60));
        assert_eq!(components["di_water_leak_1"]["expire_after"], json!(60));
        assert_eq!(components["total_amps"]["availability_mode"], json!("all"));
        assert_eq!(
            components["total_amps"]["availability"],
            combined_availability()
        );
        assert!(components["total_amps"].get("availability_topic").is_none());
    }

    #[test]
    fn fresh_cached_states_follow_layout_order_and_skip_stale_states() {
        let layout = test_layout();
        let now = Instant::now();
        let mut last_states = HashMap::new();

        last_states.insert(
            "reef/plc/state/alarms".to_string(),
            CachedState {
                payload: "{\"Alarm_Ph\":true}".to_string(),
                updated_at: now,
            },
        );
        last_states.insert(
            "reef/plc/state/di".to_string(),
            CachedState {
                payload: "{\"DI_Return_Float_High\":true}".to_string(),
                updated_at: now - Duration::from_secs(DEFAULT_TOPIC_HEALTH_EXPIRE_AFTER_SECONDS),
            },
        );
        last_states.insert(
            "reef/plc/state/inputs".to_string(),
            CachedState {
                payload: "{\"Temp_Sump_1\":78.3}".to_string(),
                updated_at: now
                    - Duration::from_secs(DEFAULT_TOPIC_HEALTH_EXPIRE_AFTER_SECONDS + 1),
            },
        );
        last_states.insert(
            "reef/plc/state/clock".to_string(),
            CachedState {
                payload: "\"2026-07-26T12:00:00-07:00\"".to_string(),
                updated_at: now - Duration::from_secs(300),
            },
        );

        let states = fresh_cached_states(&layout, &last_states, now);

        assert_eq!(
            states,
            vec![
                ("reef/plc/state/di", "{\"DI_Return_Float_High\":true}"),
                ("reef/plc/state/alarms", "{\"Alarm_Ph\":true}")
            ]
        );
    }

    #[test]
    fn reconnect_backoff_doubles_caps_and_resets() {
        let mut backoff = ReconnectBackoff::new(Duration::from_secs(1), Duration::from_secs(5));

        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
        assert_eq!(backoff.next_delay(), Duration::from_secs(2));
        assert_eq!(backoff.next_delay(), Duration::from_secs(4));
        assert_eq!(backoff.next_delay(), Duration::from_secs(5));
        assert_eq!(backoff.next_delay(), Duration::from_secs(5));

        backoff.reset();

        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
    }

    fn discovery_components(options: &AppOptions, layout: &Layout) -> Map<String, Value> {
        discovery_messages(options, layout)
            .into_iter()
            .map(|(_, payload)| {
                let component_id = payload["unique_id"]
                    .as_str()
                    .unwrap()
                    .strip_prefix(&format!("{DEVICE_ID}_"))
                    .unwrap()
                    .to_string();
                (component_id, payload)
            })
            .collect()
    }

    fn test_layout() -> Layout {
        load_layout().unwrap()
    }

    fn test_options(publish_diagnostic_ai: bool) -> AppOptions {
        AppOptions {
            mqtt_host: "mqtt.example.test".to_string(),
            mqtt_port: 1883,
            mqtt_username: String::new(),
            mqtt_password: String::new(),
            discovery_prefix: "homeassistant".to_string(),
            publish_diagnostic_ai,
            log_level: "info".to_string(),
        }
    }

    fn combined_availability() -> Value {
        json!([
            {
                "topic": "plc/aquarium/status",
                "payload_available": "online",
                "payload_not_available": "offline"
            },
            {
                "topic": "reef/plc/status",
                "payload_available": "online",
                "payload_not_available": "offline"
            }
        ])
    }
}
