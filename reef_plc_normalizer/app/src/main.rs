use std::collections::{HashMap, HashSet};
use std::fs;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, LastWill, MqttOptions, QoS};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use thiserror::Error;
use tokio::time;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

const APP_NAME: &str = "reef-plc-normalizer";
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const CLIENT_ID: &str = "reef-plc-normalizer";
const DEVICE_ID: &str = "reef_plc";
const DEVICE_NAME: &str = "Reef PLC";
const AVAILABILITY_TOPIC: &str = "reef/plc/status";
const HA_STATUS_TOPIC: &str = "homeassistant/status";
const PACKED_MQTT_LAYOUT: &str = include_str!("../packed_mqtt_layout.yaml");
const MQTT_REQUEST_CHANNEL_CAPACITY: usize = 256;
const TOPIC_HEALTH_EXPIRE_AFTER_SECONDS: u64 = 60;
const MQTT_RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const MQTT_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "/data/options.json")]
    options: String,
}

#[derive(Debug, Deserialize)]
struct AppOptions {
    mqtt_host: String,
    mqtt_port: u16,
    #[serde(default)]
    mqtt_username: String,
    #[serde(default)]
    mqtt_password: String,
    #[serde(default = "default_discovery_prefix")]
    discovery_prefix: String,
    #[serde(default)]
    publish_diagnostic_ai: bool,
    #[serde(default = "default_log_level")]
    log_level: String,
}

fn default_discovery_prefix() -> String {
    "homeassistant".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TopicKind {
    Di,
    Do,
    Ai,
    Inputs,
    Alarms,
    Ato,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ValueType {
    Bool,
    Float,
    Int,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Domain {
    BinarySensor,
    Sensor,
}

impl Domain {
    fn as_str(self) -> &'static str {
        match self {
            Self::BinarySensor => "binary_sensor",
            Self::Sensor => "sensor",
        }
    }
}

#[derive(Debug, Deserialize)]
struct Layout {
    topics: Vec<TopicSpec>,
}

#[derive(Debug, Deserialize)]
struct Field {
    source: String,
    length: usize,
    value_type: ValueType,
    active_when: Option<bool>,
    discovery: FieldDiscovery,
}

#[derive(Debug, Deserialize)]
struct FieldDiscovery {
    domain: Domain,
    name: String,
    unit_of_measurement: Option<String>,
    device_class: Option<String>,
    state_class: Option<String>,
    suggested_display_precision: Option<u8>,
    entity_category: Option<String>,
    #[serde(default = "default_enabled_by_default")]
    enabled_by_default: bool,
}

fn default_enabled_by_default() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct TopicSpec {
    kind: TopicKind,
    source_topic: String,
    state_topic: String,
    fields: Vec<Field>,
}

#[derive(Debug, Clone)]
struct CachedState {
    payload: String,
    updated_at: Instant,
}

#[derive(Debug)]
struct ReconnectBackoff {
    initial: Duration,
    current: Duration,
    max: Duration,
}

impl ReconnectBackoff {
    fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            current: initial,
            max,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = self.current.saturating_mul(2).min(self.max);
        delay
    }

    fn reset(&mut self) {
        self.current = self.initial;
    }
}

#[derive(Debug, Error)]
enum ParsePayloadError {
    #[error("field count mismatch for {topic}: expected {expected}, got {actual}")]
    CountMismatch {
        topic: String,
        expected: usize,
        actual: usize,
    },
    #[error("invalid bool value for {topic}.{field}: {value:?}")]
    InvalidBool {
        topic: String,
        field: String,
        value: String,
    },
    #[error("invalid int value for {topic}.{field}: {value:?}")]
    InvalidInt {
        topic: String,
        field: String,
        value: String,
    },
    #[error("invalid float value for {topic}.{field}: {value:?}")]
    InvalidFloat {
        topic: String,
        field: String,
        value: String,
    },
}

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

fn load_options(path: &str) -> Result<AppOptions> {
    let raw = fs::read_to_string(path).with_context(|| format!("failed to read {path}"))?;
    serde_json::from_str(&raw).with_context(|| format!("failed to parse {path}"))
}

fn load_layout() -> Result<Layout> {
    let layout: Layout = serde_yaml::from_str(PACKED_MQTT_LAYOUT)
        .context("failed to parse embedded packed MQTT layout")?;
    validate_layout(&layout)?;
    Ok(layout)
}

fn validate_layout(layout: &Layout) -> Result<()> {
    anyhow::ensure!(
        !layout.topics.is_empty(),
        "packed MQTT layout has no topics"
    );

    let mut source_topics = HashSet::new();
    let mut state_topics = HashSet::new();
    let mut field_sources = HashSet::new();

    for spec in &layout.topics {
        anyhow::ensure!(
            !spec.source_topic.trim().is_empty(),
            "packed MQTT layout has an empty source topic"
        );
        anyhow::ensure!(
            !spec.state_topic.trim().is_empty(),
            "packed MQTT layout has an empty state topic"
        );
        anyhow::ensure!(
            source_topics.insert(spec.source_topic.as_str()),
            "duplicate source topic in packed MQTT layout: {}",
            spec.source_topic
        );
        anyhow::ensure!(
            state_topics.insert(spec.state_topic.as_str()),
            "duplicate state topic in packed MQTT layout: {}",
            spec.state_topic
        );
        anyhow::ensure!(
            !spec.fields.is_empty(),
            "packed MQTT layout topic {} has no fields",
            spec.source_topic
        );

        for field in &spec.fields {
            anyhow::ensure!(
                !field.source.trim().is_empty(),
                "packed MQTT layout topic {} has an empty field source",
                spec.source_topic
            );
            anyhow::ensure!(
                field.length > 0,
                "packed MQTT layout field {} has an invalid length",
                field.source
            );
            anyhow::ensure!(
                field_sources.insert(field.source.as_str()),
                "duplicate field source in packed MQTT layout: {}",
                field.source
            );
            anyhow::ensure!(
                !field.discovery.name.trim().is_empty(),
                "packed MQTT layout field {} has an empty discovery name",
                field.source
            );

            match (field.value_type, field.discovery.domain) {
                (ValueType::Bool, Domain::BinarySensor) => {}
                (ValueType::Float | ValueType::Int, Domain::Sensor) => {}
                _ => anyhow::bail!(
                    "packed MQTT layout field {} has incompatible value_type/domain",
                    field.source
                ),
            }
            anyhow::ensure!(
                field.value_type == ValueType::Bool || field.active_when.is_none(),
                "packed MQTT layout field {} uses active_when on a non-bool field",
                field.source
            );
        }
    }

    Ok(())
}

fn init_logging(level: &str) -> Result<()> {
    let filter = EnvFilter::try_new(level).or_else(|_| EnvFilter::try_new("info"))?;
    tracing_subscriber::fmt().with_env_filter(filter).init();
    Ok(())
}

async fn run(options: AppOptions, layout: Layout) -> Result<()> {
    let mut mqtt_options =
        MqttOptions::new(CLIENT_ID, options.mqtt_host.clone(), options.mqtt_port);
    mqtt_options.set_keep_alive(Duration::from_secs(30));
    mqtt_options.set_last_will(LastWill::new(
        AVAILABILITY_TOPIC,
        "offline",
        QoS::AtLeastOnce,
        true,
    ));

    if !options.mqtt_username.is_empty() {
        mqtt_options.set_credentials(options.mqtt_username.clone(), options.mqtt_password.clone());
    }

    let (client, mut event_loop) = AsyncClient::new(mqtt_options, MQTT_REQUEST_CHANNEL_CAPACITY);

    poll_loop(client, &mut event_loop, options, layout).await
}

async fn refresh_connection(
    client: &AsyncClient,
    options: &AppOptions,
    layout: &Layout,
    last_states: &HashMap<String, CachedState>,
    now: Instant,
) -> Result<()> {
    client
        .publish(AVAILABILITY_TOPIC, QoS::AtLeastOnce, true, "online")
        .await
        .context("failed to publish availability")?;
    subscribe(client, layout).await?;
    publish_discovery(client, options, layout).await?;
    republish_fresh_states(client, layout, last_states, now).await?;
    Ok(())
}

async fn subscribe(client: &AsyncClient, layout: &Layout) -> Result<()> {
    for spec in &layout.topics {
        client
            .subscribe(spec.source_topic.as_str(), QoS::AtLeastOnce)
            .await
            .with_context(|| format!("failed to subscribe to {}", spec.source_topic))?;
    }

    client
        .subscribe(HA_STATUS_TOPIC, QoS::AtLeastOnce)
        .await
        .with_context(|| format!("failed to subscribe to {HA_STATUS_TOPIC}"))?;

    Ok(())
}

async fn republish_fresh_states(
    client: &AsyncClient,
    layout: &Layout,
    last_states: &HashMap<String, CachedState>,
    now: Instant,
) -> Result<()> {
    for (state_topic, state_payload) in fresh_cached_states(layout, last_states, now) {
        client
            .publish(
                state_topic,
                QoS::AtLeastOnce,
                false,
                state_payload.as_bytes(),
            )
            .await
            .with_context(|| format!("failed to republish {state_topic}"))?;
    }

    Ok(())
}

fn fresh_cached_states<'a>(
    layout: &'a Layout,
    last_states: &'a HashMap<String, CachedState>,
    now: Instant,
) -> Vec<(&'a str, &'a str)> {
    layout
        .topics
        .iter()
        .filter_map(|spec| {
            let cached = last_states.get(&spec.state_topic)?;
            let age = now.saturating_duration_since(cached.updated_at);
            (age <= Duration::from_secs(TOPIC_HEALTH_EXPIRE_AFTER_SECONDS))
                .then_some((spec.state_topic.as_str(), cached.payload.as_str()))
        })
        .collect()
}

async fn poll_loop(
    client: AsyncClient,
    event_loop: &mut EventLoop,
    options: AppOptions,
    layout: Layout,
) -> Result<()> {
    let mut last_states: HashMap<String, CachedState> = HashMap::new();
    let mut reconnect_backoff =
        ReconnectBackoff::new(MQTT_RECONNECT_INITIAL_DELAY, MQTT_RECONNECT_MAX_DELAY);

    loop {
        match event_loop.poll().await {
            Ok(Event::Incoming(Incoming::ConnAck(connack))) => {
                reconnect_backoff.reset();
                info!(
                    session_present = connack.session_present,
                    "MQTT connection established; refreshing subscriptions and discovery"
                );
                refresh_connection(&client, &options, &layout, &last_states, Instant::now())
                    .await?;
            }
            Ok(Event::Incoming(Incoming::Publish(packet))) => {
                let topic = packet.topic.as_str();
                let payload = String::from_utf8_lossy(&packet.payload);

                if topic == HA_STATUS_TOPIC {
                    if payload.trim() == "online" {
                        info!("Home Assistant MQTT birth received; republishing discovery");
                        publish_discovery(&client, &options, &layout).await?;
                        republish_fresh_states(&client, &layout, &last_states, Instant::now())
                            .await?;
                    }
                    continue;
                }

                let Some(spec) = layout.topics.iter().find(|spec| spec.source_topic == topic)
                else {
                    debug!(topic, "ignoring unmatched MQTT topic");
                    continue;
                };

                match parse_payload(spec, &payload) {
                    Ok(state) => {
                        let state_payload = serde_json::to_string(&state)
                            .context("failed to serialize normalized state")?;
                        client
                            .publish(
                                spec.state_topic.as_str(),
                                QoS::AtLeastOnce,
                                false,
                                state_payload.as_bytes(),
                            )
                            .await
                            .with_context(|| format!("failed to publish {}", spec.state_topic))?;
                        last_states.insert(
                            spec.state_topic.clone(),
                            CachedState {
                                payload: state_payload,
                                updated_at: Instant::now(),
                            },
                        );
                    }
                    Err(err) => {
                        warn!(%err, payload = %payload, "rejecting PLC payload");
                    }
                }
            }
            Ok(event) => {
                debug!(?event, "MQTT event");
            }
            Err(err) => {
                let delay = reconnect_backoff.next_delay();
                error!(%err, delay_seconds = delay.as_secs(), "MQTT event loop error; retrying");
                time::sleep(delay).await;
            }
        }
    }
}

fn parse_payload(spec: &TopicSpec, payload: &str) -> Result<Map<String, Value>, ParsePayloadError> {
    let mut values = split_csv(payload);
    if values.last().is_some_and(|value| value.is_empty()) {
        values.pop();
    }

    if values.len() != spec.fields.len() {
        return Err(ParsePayloadError::CountMismatch {
            topic: spec.source_topic.clone(),
            expected: spec.fields.len(),
            actual: values.len(),
        });
    }

    let mut state = Map::with_capacity(spec.fields.len());
    for (field, value) in spec.fields.iter().zip(values) {
        let parsed = parse_value(&spec.source_topic, field, value)?;
        state.insert(field.source.clone(), parsed);
    }

    Ok(state)
}

fn split_csv(payload: &str) -> Vec<&str> {
    payload.split(',').map(str::trim).collect()
}

fn parse_value(topic: &str, field: &Field, value: &str) -> Result<Value, ParsePayloadError> {
    match field.value_type {
        ValueType::Bool => parse_bool(value)
            .map(|raw| Value::Bool(raw == field.active_when.unwrap_or(true)))
            .ok_or_else(|| ParsePayloadError::InvalidBool {
                topic: topic.to_string(),
                field: field.source.clone(),
                value: value.to_string(),
            }),
        ValueType::Float => {
            value
                .parse::<f64>()
                .map(Value::from)
                .map_err(|_| ParsePayloadError::InvalidFloat {
                    topic: topic.to_string(),
                    field: field.source.clone(),
                    value: value.to_string(),
                })
        }
        ValueType::Int => {
            value
                .parse::<i64>()
                .map(Value::from)
                .map_err(|_| ParsePayloadError::InvalidInt {
                    topic: topic.to_string(),
                    field: field.source.clone(),
                    value: value.to_string(),
                })
        }
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "1" => Some(true),
        "0" => Some(false),
        value if value.eq_ignore_ascii_case("true") => Some(true),
        value if value.eq_ignore_ascii_case("false") => Some(false),
        value if value.eq_ignore_ascii_case("on") => Some(true),
        value if value.eq_ignore_ascii_case("off") => Some(false),
        _ => None,
    }
}

async fn publish_discovery(
    client: &AsyncClient,
    options: &AppOptions,
    layout: &Layout,
) -> Result<()> {
    let messages = discovery_messages(options, layout);
    info!(
        count = messages.len(),
        "publishing Home Assistant discovery"
    );

    for (topic, payload) in messages {
        let payload =
            serde_json::to_string(&payload).context("failed to serialize discovery payload")?;

        client
            .publish(topic.as_str(), QoS::AtLeastOnce, true, payload)
            .await
            .with_context(|| format!("failed to publish discovery to {topic}"))?;
    }

    Ok(())
}

fn discovery_messages(options: &AppOptions, layout: &Layout) -> Vec<(String, Value)> {
    let mut messages = Vec::new();

    for spec in &layout.topics {
        messages.push(topic_health_discovery_message(options, spec));

        if spec.kind == TopicKind::Ai && !options.publish_diagnostic_ai {
            continue;
        }

        for field in &spec.fields {
            let component_id = component_id(&field.source);
            let mut component = Map::new();
            component.insert(
                "unique_id".to_string(),
                Value::String(format!("{DEVICE_ID}_{component_id}")),
            );
            component.insert(
                "name".to_string(),
                Value::String(field.discovery.name.clone()),
            );
            component.insert(
                "state_topic".to_string(),
                Value::String(spec.state_topic.to_string()),
            );
            component.insert(
                "availability_topic".to_string(),
                Value::String(AVAILABILITY_TOPIC.to_string()),
            );
            component.insert(
                "payload_available".to_string(),
                Value::String("online".to_string()),
            );
            component.insert(
                "payload_not_available".to_string(),
                Value::String("offline".to_string()),
            );

            match field.value_type {
                ValueType::Bool => {
                    component.insert(
                        "value_template".to_string(),
                        Value::String(format!(
                            "{{{{ 'ON' if value_json[{}] else 'OFF' }}}}",
                            jinja_key(&field.source)
                        )),
                    );
                    component.insert("payload_on".to_string(), Value::String("ON".to_string()));
                    component.insert("payload_off".to_string(), Value::String("OFF".to_string()));
                }
                ValueType::Float | ValueType::Int => {
                    component.insert(
                        "value_template".to_string(),
                        Value::String(format!(
                            "{{{{ value_json[{}] }}}}",
                            jinja_key(&field.source)
                        )),
                    );
                }
            }

            if let Some(unit) = &field.discovery.unit_of_measurement {
                component.insert(
                    "unit_of_measurement".to_string(),
                    Value::String(unit.clone()),
                );
            }
            if let Some(device_class) = &field.discovery.device_class {
                component.insert(
                    "device_class".to_string(),
                    Value::String(device_class.clone()),
                );
            }
            if let Some(state_class) = &field.discovery.state_class {
                component.insert(
                    "state_class".to_string(),
                    Value::String(state_class.clone()),
                );
            }
            if let Some(suggested_display_precision) = field.discovery.suggested_display_precision {
                component.insert(
                    "suggested_display_precision".to_string(),
                    Value::from(suggested_display_precision),
                );
            }
            if let Some(entity_category) = &field.discovery.entity_category {
                component.insert(
                    "entity_category".to_string(),
                    Value::String(entity_category.clone()),
                );
            }
            if !field.discovery.enabled_by_default {
                component.insert("enabled_by_default".to_string(), Value::Bool(false));
            }
            component.insert("device".to_string(), device_payload());
            component.insert("origin".to_string(), origin_payload());

            let discovery_topic = format!(
                "{}/{}/{}/config",
                options.discovery_prefix,
                field.discovery.domain.as_str(),
                format!("{DEVICE_ID}_{component_id}")
            );
            messages.push((discovery_topic, Value::Object(component)));
        }
    }

    messages
}

fn topic_health_discovery_message(options: &AppOptions, spec: &TopicSpec) -> (String, Value) {
    let component_id = format!("{}_topic_online", spec.kind.as_str());
    let payload = json!({
        "unique_id": format!("{DEVICE_ID}_{component_id}"),
        "name": format!("{} Topic Online", spec.kind.display_name()),
        "state_topic": spec.state_topic.as_str(),
        "value_template": "ON",
        "payload_on": "ON",
        "expire_after": TOPIC_HEALTH_EXPIRE_AFTER_SECONDS,
        "availability_topic": AVAILABILITY_TOPIC,
        "payload_available": "online",
        "payload_not_available": "offline",
        "device_class": "connectivity",
        "entity_category": "diagnostic",
        "device": device_payload(),
        "origin": origin_payload(),
    });
    let discovery_topic = format!(
        "{}/binary_sensor/{DEVICE_ID}_{component_id}/config",
        options.discovery_prefix
    );

    (discovery_topic, payload)
}

fn device_payload() -> Value {
    json!({
        "identifiers": [DEVICE_ID],
        "name": DEVICE_NAME,
        "manufacturer": "AutomationDirect",
        "model": "P1-550"
    })
}

fn origin_payload() -> Value {
    json!({
        "name": APP_NAME,
        "sw_version": APP_VERSION,
        "support_url": "https://github.com/ianhattendorf/reef-plc-normalizer/tree/main/reef_plc_normalizer"
    })
}

fn component_id(source: &str) -> String {
    source
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn jinja_key(source: &str) -> String {
    serde_json::to_string(source).expect("source string should serialize")
}

impl TopicKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Di => "di",
            Self::Do => "do",
            Self::Ai => "ai",
            Self::Inputs => "inputs",
            Self::Alarms => "alarms",
            Self::Ato => "ato",
        }
    }

    fn display_name(self) -> &'static str {
        match self {
            Self::Di => "DI",
            Self::Do => "DO",
            Self::Ai => "AI",
            Self::Inputs => "Inputs",
            Self::Alarms => "Alarms",
            Self::Ato => "ATO",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn embedded_layout_loads_and_validates() {
        let layout = test_layout();

        assert_eq!(layout.topics.len(), 6);
        assert!(layout
            .topics
            .iter()
            .any(|spec| spec.source_topic == "plc/aquarium/inputs"));
        assert!(layout
            .topics
            .iter()
            .any(|spec| spec.source_topic == "plc/aquarium/alarms"));

        let di = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Di)
            .unwrap();
        assert_eq!(di.fields[4].source, "DI_Return_Float_LowLow");
        assert_eq!(di.fields[4].length, 1);

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
        assert_eq!(alarms.fields.len(), 16);
        assert_eq!(alarms.fields[0].source, "Alarm_Heater_Not_On");
        assert_eq!(alarms.fields[11].source, "Alarm_ATO_Runtime");
        assert_eq!(alarms.fields[12].source, "Alarm_Heater_1_On_Time");
        assert_eq!(alarms.fields[13].source, "Alarm_Ph");
        assert_eq!(alarms.fields[14].source, "Alarm_Return_Float_Low_Time");
        assert_eq!(alarms.fields[15].source, "Alarm_Not_Auto_Mode_Time");

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
    }

    #[test]
    fn parses_alarm_payloads() {
        let layout = test_layout();
        let spec = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Alarms)
            .unwrap();
        let state = parse_payload(spec, "1,0,0,1,0,1,0,0,1,0,1,0,1,0,1,0,").unwrap();

        assert_eq!(state["Alarm_Heater_Not_On"], json!(true));
        assert_eq!(state["Alarm_Heater_On"], json!(false));
        assert_eq!(state["Alarm_Return_Pump_Not_Running"], json!(true));
        assert_eq!(state["Alarm_ATO_Runtime"], json!(false));
        assert_eq!(state["Alarm_Heater_1_On_Time"], json!(true));
        assert_eq!(state["Alarm_Ph"], json!(false));
        assert_eq!(state["Alarm_Return_Float_Low_Time"], json!(true));
        assert_eq!(state["Alarm_Not_Auto_Mode_Time"], json!(false));
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
    fn parses_inputs_with_trailing_comma() {
        let layout = test_layout();
        let spec = layout
            .topics
            .iter()
            .find(|spec| spec.kind == TopicKind::Inputs)
            .unwrap();
        let state = parse_payload(
            spec,
            "78.3,78.1,78.3,0.57,0.40,0.00,0.41,0.35,0.12,0.10,0,1,1,0.05,1,8.12,",
        )
        .unwrap();

        assert_eq!(state["Temp_Sump_1"], json!(78.3));
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
            assert_eq!(components[component_id]["expire_after"], json!(60));
            assert_eq!(
                components[component_id]["availability_topic"],
                json!("reef/plc/status")
            );
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
                updated_at: now - Duration::from_secs(TOPIC_HEALTH_EXPIRE_AFTER_SECONDS),
            },
        );
        last_states.insert(
            "reef/plc/state/inputs".to_string(),
            CachedState {
                payload: "{\"Temp_Sump_1\":78.3}".to_string(),
                updated_at: now - Duration::from_secs(TOPIC_HEALTH_EXPIRE_AFTER_SECONDS + 1),
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
}
