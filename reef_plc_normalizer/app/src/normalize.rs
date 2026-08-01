use std::time::{SystemTime, UNIX_EPOCH};

use chrono::DateTime;
use serde_json::{Map, Value};
use thiserror::Error;

use crate::layout::{Field, TopicKind, TopicSpec, ValueType};

pub(super) const CLOCK_OFFSET_FIELD: &str = "Clock_Offset_Seconds";

#[derive(Debug, Error)]
pub(super) enum ParsePayloadError {
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

pub(super) fn parse_payload(
    spec: &TopicSpec,
    payload: &str,
) -> Result<Map<String, Value>, ParsePayloadError> {
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

pub(super) fn normalize_payload(
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
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
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
