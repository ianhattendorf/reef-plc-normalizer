use std::collections::HashSet;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::contract;

const PACKED_MQTT_LAYOUT: &str = include_str!("../packed_mqtt_layout.yaml");

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum TopicKind {
    Di,
    Do,
    Ai,
    Inputs,
    Alarms,
    Ato,
    TimeSync,
    Gmp40,
    Clock,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum ValueType {
    Bool,
    Float,
    Int,
    Timestamp,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub(super) enum Domain {
    BinarySensor,
    Light,
    Switch,
    Select,
    Number,
    Sensor,
}

impl Domain {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::BinarySensor => "binary_sensor",
            Self::Light => "light",
            Self::Switch => "switch",
            Self::Select => "select",
            Self::Number => "number",
            Self::Sensor => "sensor",
        }
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct Layout {
    #[serde(default)]
    pub(super) removed_discovery: Vec<RemovedDiscovery>,
    pub(super) topics: Vec<TopicSpec>,
}

#[derive(Debug, Deserialize)]
pub(super) struct RemovedDiscovery {
    pub(super) domain: Domain,
    pub(super) object_id: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct Field {
    pub(super) source: String,
    #[serde(default)]
    pub(super) plc_source: Option<String>,
    pub(super) length: usize,
    pub(super) value_type: ValueType,
    pub(super) active_when: Option<bool>,
    pub(super) discovery: FieldDiscovery,
}

#[derive(Debug, Deserialize)]
pub(super) struct FieldDiscovery {
    pub(super) domain: Domain,
    pub(super) name: String,
    pub(super) component_id: Option<String>,
    pub(super) default_entity_id: Option<String>,
    pub(super) command_topic: Option<String>,
    pub(super) command_mask: Option<u8>,
    pub(super) min: Option<i64>,
    pub(super) max: Option<i64>,
    pub(super) options: Option<Vec<String>>,
    pub(super) unit_of_measurement: Option<String>,
    pub(super) device_class: Option<String>,
    pub(super) state_class: Option<String>,
    pub(super) suggested_display_precision: Option<u8>,
    pub(super) entity_category: Option<String>,
    #[serde(default = "default_enabled_by_default")]
    pub(super) enabled_by_default: bool,
}

fn default_enabled_by_default() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub(super) struct TopicSpec {
    pub(super) kind: TopicKind,
    pub(super) source_topic: String,
    pub(super) state_topic: String,
    pub(super) raw_command_topic: Option<String>,
    pub(super) fields: Vec<Field>,
}

pub(super) fn load_layout() -> Result<Layout> {
    let layout: Layout = serde_yaml::from_str(PACKED_MQTT_LAYOUT)
        .context("failed to parse embedded packed MQTT layout")?;
    validate_layout(&layout)?;
    contract::validate_layout_contract(&layout)?;
    Ok(layout)
}

pub(super) fn validate_layout(layout: &Layout) -> Result<()> {
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
                (ValueType::Bool, Domain::BinarySensor | Domain::Light | Domain::Switch) => {}
                (ValueType::Int, Domain::Sensor | Domain::Select | Domain::Number) => {}
                (ValueType::Float | ValueType::Timestamp, Domain::Sensor) => {}
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
                Domain::Light | Domain::Switch | Domain::Select | Domain::Number => {
                    anyhow::ensure!(
                        field
                            .discovery
                            .command_topic
                            .as_deref()
                            .is_some_and(|topic| !topic.trim().is_empty()),
                        "packed MQTT controllable field {} requires command_topic",
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
            if field.discovery.command_mask.is_some() {
                anyhow::ensure!(
                    spec.raw_command_topic.is_some(),
                    "encoded command field {} requires raw_command_topic",
                    field.source
                );
                anyhow::ensure!(
                    matches!(
                        field.discovery.domain,
                        Domain::Switch | Domain::Select | Domain::Number
                    ),
                    "encoded command field {} has invalid domain",
                    field.source
                );
            }
        }
    }

    Ok(())
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

pub(super) fn field_component_id(field: &Field) -> String {
    field
        .discovery
        .component_id
        .clone()
        .unwrap_or_else(|| component_id(&field.source))
}
