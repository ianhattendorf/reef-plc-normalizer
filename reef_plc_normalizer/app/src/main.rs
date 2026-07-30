use std::collections::{HashMap, HashSet};
use std::fs;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use chrono::DateTime;
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
const PLC_AVAILABILITY_TOPIC: &str = "plc/aquarium/status";
const HA_STATUS_TOPIC: &str = "homeassistant/status";
const PACKED_MQTT_LAYOUT: &str = include_str!("../packed_mqtt_layout.yaml");
const MQTT_REQUEST_CHANNEL_CAPACITY: usize = 256;
const DEFAULT_TOPIC_HEALTH_EXPIRE_AFTER_SECONDS: u64 = 60;
const CLOCK_TOPIC_HEALTH_EXPIRE_AFTER_SECONDS: u64 = 390;
const CACHED_STATE_REPLAY_MAX_AGE_SECONDS: u64 = 60;
const CLOCK_OFFSET_FIELD: &str = "Clock_Offset_Seconds";
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
    TimeSync,
    Clock,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ValueType {
    Bool,
    Float,
    Int,
    Timestamp,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
enum Domain {
    BinarySensor,
    Light,
    Sensor,
}

impl Domain {
    fn as_str(self) -> &'static str {
        match self {
            Self::BinarySensor => "binary_sensor",
            Self::Light => "light",
            Self::Sensor => "sensor",
        }
    }
}

#[derive(Debug, Deserialize)]
struct Layout {
    #[serde(default)]
    removed_discovery: Vec<RemovedDiscovery>,
    topics: Vec<TopicSpec>,
}

#[derive(Debug, Deserialize)]
struct RemovedDiscovery {
    domain: Domain,
    object_id: String,
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
    component_id: Option<String>,
    default_entity_id: Option<String>,
    command_topic: Option<String>,
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
    #[error("invalid timestamp value for {topic}.{field}: {value:?}")]
    InvalidTimestamp {
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
    let mut component_ids = HashSet::new();
    let mut removed_discovery_topics = HashSet::new();

    for removed in &layout.removed_discovery {
        anyhow::ensure!(
            !removed.object_id.trim().is_empty(),
            "removed discovery object_id cannot be empty"
        );
        anyhow::ensure!(
            removed_discovery_topics.insert((removed.domain, removed.object_id.as_str())),
            "duplicate removed discovery topic: {}/{}",
            removed.domain.as_str(),
            removed.object_id
        );
    }

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
            let component_id = field_component_id(field);
            anyhow::ensure!(
                !component_id.trim().is_empty(),
                "packed MQTT layout field {} has an empty component_id",
                field.source
            );
            anyhow::ensure!(
                component_ids.insert(component_id.clone()),
                "duplicate discovery component_id in packed MQTT layout: {component_id}"
            );

            match (field.value_type, field.discovery.domain) {
                (ValueType::Bool, Domain::BinarySensor | Domain::Light) => {}
                (ValueType::Float | ValueType::Int | ValueType::Timestamp, Domain::Sensor) => {}
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
            match field.discovery.domain {
                Domain::Light => {
                    anyhow::ensure!(
                        field
                            .discovery
                            .command_topic
                            .as_deref()
                            .is_some_and(|topic| !topic.trim().is_empty()),
                        "packed MQTT light {} requires command_topic",
                        field.source
                    );
                }
                Domain::BinarySensor | Domain::Sensor => {
                    anyhow::ensure!(
                        field.discovery.command_topic.is_none(),
                        "packed MQTT layout field {} uses command_topic outside the light domain",
                        field.source
                    );
                }
            }
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
            (age <= Duration::from_secs(CACHED_STATE_REPLAY_MAX_AGE_SECONDS))
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

                match normalize_payload(spec, &payload, SystemTime::now()) {
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

fn normalize_payload(
    spec: &TopicSpec,
    payload: &str,
    received_at: SystemTime,
) -> Result<Map<String, Value>, ParsePayloadError> {
    let mut state = parse_payload(spec, payload)?;

    if spec.kind == TopicKind::Clock {
        let plc_clock = state
            .get("PLC_Clock")
            .and_then(Value::as_str)
            .ok_or_else(|| ParsePayloadError::InvalidTimestamp {
                topic: spec.source_topic.clone(),
                field: "PLC_Clock".to_string(),
                value: String::new(),
            })?;
        let plc_timestamp = DateTime::parse_from_rfc3339(plc_clock).map_err(|_| {
            ParsePayloadError::InvalidTimestamp {
                topic: spec.source_topic.clone(),
                field: "PLC_Clock".to_string(),
                value: plc_clock.to_string(),
            }
        })?;
        let receipt_seconds = system_time_unix_seconds(received_at);
        let plc_seconds = plc_timestamp.timestamp_millis() as f64 / 1_000.0;
        let offset_seconds = ((receipt_seconds - plc_seconds) * 1_000.0).round() / 1_000.0;

        state.insert(CLOCK_OFFSET_FIELD.to_string(), Value::from(offset_seconds));
    }

    Ok(state)
}

fn system_time_unix_seconds(timestamp: SystemTime) -> f64 {
    match timestamp.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs_f64(),
        Err(err) => -err.duration().as_secs_f64(),
    }
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
        ValueType::Timestamp => {
            if is_plc_timestamp(value) {
                Ok(Value::String(value.to_string()))
            } else {
                Err(ParsePayloadError::InvalidTimestamp {
                    topic: topic.to_string(),
                    field: field.source.clone(),
                    value: value.to_string(),
                })
            }
        }
    }
}

fn is_plc_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 25
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || !matches!(bytes[19], b'+' | b'-')
        || bytes[22] != b':'
    {
        return false;
    }

    let Some(year) = parse_digits(bytes, 0, 4) else {
        return false;
    };
    let Some(month) = parse_digits(bytes, 5, 2) else {
        return false;
    };
    let Some(day) = parse_digits(bytes, 8, 2) else {
        return false;
    };
    let Some(hour) = parse_digits(bytes, 11, 2) else {
        return false;
    };
    let Some(minute) = parse_digits(bytes, 14, 2) else {
        return false;
    };
    let Some(second) = parse_digits(bytes, 17, 2) else {
        return false;
    };
    let Some(offset_hour) = parse_digits(bytes, 20, 2) else {
        return false;
    };
    let Some(offset_minute) = parse_digits(bytes, 23, 2) else {
        return false;
    };

    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => return false,
    };

    (1..=days_in_month).contains(&day)
        && hour <= 23
        && minute <= 59
        && second <= 59
        && offset_hour <= 23
        && offset_minute <= 59
}

fn parse_digits(bytes: &[u8], start: usize, length: usize) -> Option<u32> {
    bytes
        .get(start..start + length)?
        .iter()
        .try_fold(0_u32, |value, digit| {
            digit
                .is_ascii_digit()
                .then_some(value * 10 + u32::from(digit - b'0'))
        })
}

fn is_leap_year(year: u32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
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
    let removed_topics = removed_discovery_topics(options, layout);
    let messages = discovery_messages(options, layout);
    info!(
        count = messages.len(),
        removed_count = removed_topics.len(),
        "publishing Home Assistant discovery"
    );

    for topic in removed_topics {
        client
            .publish(topic.as_str(), QoS::AtLeastOnce, true, Vec::<u8>::new())
            .await
            .with_context(|| format!("failed to remove legacy discovery at {topic}"))?;
    }

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

fn removed_discovery_topics(options: &AppOptions, layout: &Layout) -> Vec<String> {
    layout
        .removed_discovery
        .iter()
        .map(|removed| {
            format!(
                "{}/{}/{}/config",
                options.discovery_prefix,
                removed.domain.as_str(),
                removed.object_id
            )
        })
        .collect()
}

fn discovery_messages(options: &AppOptions, layout: &Layout) -> Vec<(String, Value)> {
    let mut messages = Vec::new();

    for spec in &layout.topics {
        messages.push(topic_health_discovery_message(options, spec));
        if spec.kind == TopicKind::Clock {
            messages.push(clock_offset_discovery_message(options, spec));
        }

        if spec.kind == TopicKind::Ai && !options.publish_diagnostic_ai {
            continue;
        }

        for field in &spec.fields {
            let component_id = field_component_id(field);
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
            insert_availability(&mut component, field.discovery.domain);

            match field.value_type {
                ValueType::Bool => {
                    component.insert(
                        if field.discovery.domain == Domain::Light {
                            "state_value_template".to_string()
                        } else {
                            "value_template".to_string()
                        },
                        Value::String(format!(
                            "{{{{ 'ON' if value_json[{}] else 'OFF' }}}}",
                            jinja_key(&field.source)
                        )),
                    );
                    component.insert("payload_on".to_string(), Value::String("ON".to_string()));
                    component.insert("payload_off".to_string(), Value::String("OFF".to_string()));
                }
                ValueType::Float | ValueType::Int | ValueType::Timestamp => {
                    component.insert(
                        "value_template".to_string(),
                        Value::String(format!(
                            "{{{{ value_json[{}] }}}}",
                            jinja_key(&field.source)
                        )),
                    );
                }
            }

            if field.discovery.domain == Domain::Light {
                component.insert(
                    "command_topic".to_string(),
                    Value::String(
                        field
                            .discovery
                            .command_topic
                            .clone()
                            .expect("validated MQTT light command_topic"),
                    ),
                );
                component.insert("optimistic".to_string(), Value::Bool(false));
                component.insert("qos".to_string(), Value::from(1));
                component.insert("retain".to_string(), Value::Bool(false));
            }
            if let Some(default_entity_id) = &field.discovery.default_entity_id {
                component.insert(
                    "default_entity_id".to_string(),
                    Value::String(default_entity_id.clone()),
                );
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
                "{}/{}/{DEVICE_ID}_{component_id}/config",
                options.discovery_prefix,
                field.discovery.domain.as_str()
            );
            messages.push((discovery_topic, Value::Object(component)));
        }
    }

    messages
}

fn insert_availability(component: &mut Map<String, Value>, domain: Domain) {
    if domain == Domain::Light {
        component.insert(
            "availability".to_string(),
            json!([
                {
                    "topic": PLC_AVAILABILITY_TOPIC,
                    "payload_available": "online",
                    "payload_not_available": "offline"
                },
                {
                    "topic": AVAILABILITY_TOPIC,
                    "payload_available": "online",
                    "payload_not_available": "offline"
                }
            ]),
        );
        component.insert("availability_mode".to_string(), json!("all"));
    } else {
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
    }
}

fn clock_offset_discovery_message(options: &AppOptions, spec: &TopicSpec) -> (String, Value) {
    let component_id = "clock_offset_seconds";
    let payload = json!({
        "unique_id": format!("{DEVICE_ID}_{component_id}"),
        "name": "Clock Offset",
        "state_topic": spec.state_topic.as_str(),
        "value_template": format!("{{{{ value_json[{}] }}}}", jinja_key(CLOCK_OFFSET_FIELD)),
        "unit_of_measurement": "s",
        "state_class": "measurement",
        "suggested_display_precision": 1,
        "availability_topic": AVAILABILITY_TOPIC,
        "payload_available": "online",
        "payload_not_available": "offline",
        "entity_category": "diagnostic",
        "device": device_payload(),
        "origin": origin_payload(),
    });
    let discovery_topic = format!(
        "{}/sensor/{DEVICE_ID}_{component_id}/config",
        options.discovery_prefix
    );

    (discovery_topic, payload)
}

fn topic_health_discovery_message(options: &AppOptions, spec: &TopicSpec) -> (String, Value) {
    let component_id = format!("{}_topic_online", spec.kind.as_str());
    let payload = json!({
        "unique_id": format!("{DEVICE_ID}_{component_id}"),
        "name": format!("{} Topic Online", spec.kind.display_name()),
        "state_topic": spec.state_topic.as_str(),
        "value_template": "ON",
        "payload_on": "ON",
        "expire_after": spec.kind.topic_health_expire_after_seconds(),
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

fn field_component_id(field: &Field) -> String {
    field
        .discovery
        .component_id
        .clone()
        .unwrap_or_else(|| component_id(&field.source))
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
            Self::TimeSync => "time_sync",
            Self::Clock => "clock",
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
            Self::TimeSync => "Time Sync",
            Self::Clock => "Clock",
        }
    }

    fn topic_health_expire_after_seconds(self) -> u64 {
        match self {
            Self::Clock => CLOCK_TOPIC_HEALTH_EXPIRE_AFTER_SECONDS,
            _ => DEFAULT_TOPIC_HEALTH_EXPIRE_AFTER_SECONDS,
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

        assert_eq!(layout.topics.len(), 8);
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
        assert_eq!(cabinet_light["optimistic"], json!(false));
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
            .contains("packed MQTT light DO_Relay_DC_4 requires command_topic"));
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
}
