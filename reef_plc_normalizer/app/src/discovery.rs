use anyhow::{Context, Result};
use rumqttc::{AsyncClient, QoS};
use serde_json::{json, Map, Value};
use tracing::info;

use crate::config::AppOptions;
use crate::layout::{field_component_id, Domain, Layout, TopicKind, TopicSpec, ValueType};
use crate::normalize::CLOCK_OFFSET_FIELD;
use crate::{
    APP_NAME, APP_VERSION, AVAILABILITY_TOPIC, CLOCK_TOPIC_HEALTH_EXPIRE_AFTER_SECONDS,
    DEFAULT_TOPIC_HEALTH_EXPIRE_AFTER_SECONDS, DEVICE_ID, DEVICE_NAME, PLC_AVAILABILITY_TOPIC,
};

pub(super) async fn publish_discovery(
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

pub(super) fn removed_discovery_topics(options: &AppOptions, layout: &Layout) -> Vec<String> {
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

pub(super) fn discovery_messages(options: &AppOptions, layout: &Layout) -> Vec<(String, Value)> {
    let mut messages = vec![plc_mqtt_connected_discovery_message(options)];

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
            insert_combined_availability(&mut component);
            if field.discovery.domain != Domain::Light {
                component.insert(
                    "expire_after".to_string(),
                    Value::from(spec.kind.topic_health_expire_after_seconds()),
                );
            }

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
                component.insert("optimistic".to_string(), Value::Bool(true));
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

fn insert_combined_availability(component: &mut Map<String, Value>) {
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
}

fn plc_mqtt_connected_discovery_message(options: &AppOptions) -> (String, Value) {
    let component_id = "mqtt_connected";
    let payload = json!({
        "unique_id": format!("{DEVICE_ID}_{component_id}"),
        "name": "MQTT Connected",
        "state_topic": PLC_AVAILABILITY_TOPIC,
        "payload_on": "online",
        "payload_off": "offline",
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

fn clock_offset_discovery_message(options: &AppOptions, spec: &TopicSpec) -> (String, Value) {
    let component_id = "clock_offset_seconds";
    let mut payload = json!({
        "unique_id": format!("{DEVICE_ID}_{component_id}"),
        "name": "Clock Offset",
        "state_topic": spec.state_topic.as_str(),
        "value_template": format!("{{{{ value_json[{}] }}}}", jinja_key(CLOCK_OFFSET_FIELD)),
        "unit_of_measurement": "s",
        "state_class": "measurement",
        "suggested_display_precision": 1,
        "expire_after": spec.kind.topic_health_expire_after_seconds(),
        "entity_category": "diagnostic",
        "device": device_payload(),
        "origin": origin_payload(),
    });
    insert_combined_availability(payload.as_object_mut().unwrap());
    let discovery_topic = format!(
        "{}/sensor/{DEVICE_ID}_{component_id}/config",
        options.discovery_prefix
    );

    (discovery_topic, payload)
}

fn topic_health_discovery_message(options: &AppOptions, spec: &TopicSpec) -> (String, Value) {
    let component_id = format!("{}_topic_online", spec.kind.as_str());
    let mut payload = json!({
        "unique_id": format!("{DEVICE_ID}_{component_id}"),
        "name": format!("{} Topic Online", spec.kind.display_name()),
        "state_topic": spec.state_topic.as_str(),
        "value_template": "ON",
        "payload_on": "ON",
        "expire_after": spec.kind.topic_health_expire_after_seconds(),
        "device_class": "connectivity",
        "entity_category": "diagnostic",
        "device": device_payload(),
        "origin": origin_payload(),
    });
    insert_combined_availability(payload.as_object_mut().unwrap());
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
