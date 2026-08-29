use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::layout::{Domain, Layout, ValueType};
use crate::PLC_AVAILABILITY_TOPIC;

const PLC_CONTRACT: &str = include_str!("../contracts/plc_mqtt.json");

#[derive(Debug, Deserialize)]
struct PlcContract {
    schema_version: u64,
    generated_by: String,
    source: ContractSource,
    mqtt: MqttContract,
    packed_payloads: Vec<PackedPayload>,
}

#[derive(Debug, Deserialize)]
struct ContractSource {
    file: String,
    sha256: String,
}

#[derive(Debug, Deserialize)]
struct MqttContract {
    last_will: LastWill,
    publishers: Vec<Publisher>,
    subscribers: Vec<Subscriber>,
}

#[derive(Debug, Deserialize)]
struct LastWill {
    enabled: bool,
    qos: u8,
    retain: bool,
    topic: String,
    payload: String,
}

#[derive(Debug, Deserialize)]
struct Publisher {
    interval_seconds: u64,
    qos: u8,
    mappings: Vec<MqttMapping>,
}

#[derive(Debug, Deserialize)]
struct Subscriber {
    qos: u8,
    mappings: Vec<MqttMapping>,
}

#[derive(Debug, Deserialize)]
struct MqttMapping {
    topic: String,
    #[serde(default)]
    retain: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct PackedPayload {
    topic: String,
    fields: Vec<PackedField>,
}

#[derive(Debug, Deserialize)]
struct PackedField {
    source: String,
    length: usize,
    wire_type: String,
}

pub(super) fn validate_layout_contract(layout: &Layout) -> Result<()> {
    let contract: PlcContract =
        serde_json::from_str(PLC_CONTRACT).context("failed to parse embedded PLC MQTT contract")?;
    anyhow::ensure!(
        contract.schema_version == 1,
        "unsupported PLC contract schema"
    );
    anyhow::ensure!(
        contract.generated_by == "aquarium-controller-plc-mqtt-contract",
        "unexpected PLC contract generator"
    );
    anyhow::ensure!(
        contract.source.file == "aquarium_controller.adpro"
            && contract.source.sha256.len() == 64
            && contract
                .source
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()),
        "PLC contract has invalid source provenance"
    );
    anyhow::ensure!(
        contract.mqtt.last_will.enabled
            && contract.mqtt.last_will.topic == PLC_AVAILABILITY_TOPIC
            && contract.mqtt.last_will.payload == "offline"
            && contract.mqtt.last_will.qos == 1
            && contract.mqtt.last_will.retain,
        "PLC contract does not provide the required retained offline LWT"
    );

    let mut publications = HashMap::new();
    for publisher in &contract.mqtt.publishers {
        for mapping in &publisher.mappings {
            anyhow::ensure!(
                publications
                    .insert(
                        mapping.topic.as_str(),
                        (publisher.interval_seconds, publisher.qos, mapping.retain),
                    )
                    .is_none(),
                "duplicate PLC publication topic: {}",
                mapping.topic
            );
        }
    }
    for spec in &layout.topics {
        let Some((interval, qos, retain)) = publications.get(spec.source_topic.as_str()) else {
            anyhow::bail!(
                "layout topic missing from PLC contract: {}",
                spec.source_topic
            );
        };
        let expected_interval = if spec.source_topic.ends_with("/clock") {
            300
        } else if spec.source_topic.ends_with("/gmp40_1") {
            1
        } else {
            10
        };
        let expected_qos = if spec.source_topic.ends_with("/clock") {
            1
        } else {
            0
        };
        anyhow::ensure!(
            *interval == expected_interval && *qos == expected_qos && *retain == Some(false),
            "PLC publication metadata differs for {}",
            spec.source_topic
        );
    }
    anyhow::ensure!(
        publications.get(PLC_AVAILABILITY_TOPIC) == Some(&(10, 0, Some(true))),
        "PLC online status publication must be retained"
    );

    let mut subscribed = HashSet::new();
    for subscriber in &contract.mqtt.subscribers {
        anyhow::ensure!(
            subscriber.qos == 1,
            "PLC command subscription must use QoS 1"
        );
        for mapping in &subscriber.mappings {
            subscribed.insert(mapping.topic.as_str());
        }
    }
    for command_topic in layout.topics.iter().flat_map(|spec| {
        spec.raw_command_topic.iter().map(String::as_str).chain(
            spec.fields
                .iter()
                .filter(|field| field.discovery.command_mask.is_none())
                .filter_map(|field| field.discovery.command_topic.as_deref()),
        )
    }) {
        anyhow::ensure!(
            subscribed.contains(command_topic),
            "normalizer command topic missing from PLC contract: {command_topic}"
        );
    }

    let packed: HashMap<_, _> = contract
        .packed_payloads
        .iter()
        .map(|payload| (payload.topic.as_str(), payload))
        .collect();
    for spec in layout
        .topics
        .iter()
        .filter(|spec| !spec.source_topic.ends_with("/clock"))
    {
        let payload = packed
            .get(spec.source_topic.as_str())
            .with_context(|| format!("packed PLC contract missing {}", spec.source_topic))?;
        anyhow::ensure!(
            payload.fields.len() == spec.fields.len(),
            "packed field count differs for {}: PLC {}, normalizer {}",
            spec.source_topic,
            payload.fields.len(),
            spec.fields.len()
        );
        for (plc, field) in payload.fields.iter().zip(&spec.fields) {
            let source = field.plc_source.as_deref().unwrap_or(&field.source);
            anyhow::ensure!(
                plc.source == source && plc.length == field.length,
                "packed field differs for {}: PLC {}:{}, normalizer {}:{}",
                spec.source_topic,
                plc.source,
                plc.length,
                source,
                field.length
            );
            let normalizer_type = match field.value_type {
                ValueType::Bool => "bool",
                ValueType::Float => "float",
                ValueType::Int => "int",
                ValueType::Timestamp => "timestamp",
            };
            let compatible = plc.wire_type == normalizer_type
                || matches!(
                    (plc.wire_type.as_str(), normalizer_type),
                    ("int", "float") | ("float", "int")
                );
            anyhow::ensure!(
                compatible,
                "packed field type differs for {}.{}: PLC {}, normalizer {}",
                spec.source_topic,
                plc.source,
                plc.wire_type,
                normalizer_type
            );
            anyhow::ensure!(
                !matches!(field.discovery.domain, Domain::Light)
                    || field.value_type == ValueType::Bool,
                "contract-backed light field must be boolean"
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::load_layout;

    #[test]
    fn vendored_contract_matches_embedded_layout() {
        load_layout().unwrap();
    }
}
