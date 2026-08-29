use std::collections::{HashMap, VecDeque};

use anyhow::{anyhow, Result};
use serde_json::{Map, Value};

use crate::layout::{Domain, Layout};

const VERSION: u8 = 1;
const RECEIVE_COUNTER: &str = "GMP40_1_Authority.MQTTReceivedLast";
const COMMAND_PENDING: &str = "GMP40_1_Data.Command.Pending";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EncodedCommand {
    pub(super) raw_topic: String,
    pub(super) payload: [u8; 7],
}

#[derive(Debug, Default)]
pub(super) struct CommandQueue {
    counter: Option<i64>,
    in_flight_after: Option<i64>,
    pending: HashMap<String, EncodedCommand>,
    order: VecDeque<String>,
}

impl CommandQueue {
    pub(super) fn enqueue(&mut self, layout: &Layout, topic: &str, value: &[u8]) -> Result<()> {
        let (spec, field) = layout
            .topics
            .iter()
            .flat_map(|spec| spec.fields.iter().map(move |field| (spec, field)))
            .find(|(_, field)| field.discovery.command_topic.as_deref() == Some(topic))
            .ok_or_else(|| anyhow!("unknown command topic {topic}"))?;
        let mask = field
            .discovery
            .command_mask
            .ok_or_else(|| anyhow!("command topic {topic} is not encoded by the normalizer"))?;
        let raw_topic = spec
            .raw_command_topic
            .clone()
            .ok_or_else(|| anyhow!("command topic {topic} has no raw command topic"))?;
        let text = std::str::from_utf8(value)?.trim();
        let parsed = match field.discovery.domain {
            Domain::Switch => match text.to_ascii_uppercase().as_str() {
                "ON" => 1,
                "OFF" => 0,
                _ => return Err(anyhow!("expected ON or OFF")),
            },
            Domain::Select | Domain::Number => text
                .parse::<i64>()
                .map_err(|_| anyhow!("expected an integer"))?,
            _ => return Err(anyhow!("unsupported encoded command domain")),
        };
        if let Some(min) = field.discovery.min {
            anyhow::ensure!(parsed >= min, "value below minimum {min}");
        }
        if let Some(max) = field.discovery.max {
            anyhow::ensure!(parsed <= max, "value above maximum {max}");
        }
        if let Some(options) = &field.discovery.options {
            anyhow::ensure!(
                options.iter().any(|option| option == text),
                "value is not a valid option"
            );
        }

        let mut payload = [0; 7];
        payload[0] = VERSION;
        payload[1] = mask;
        let byte = u8::try_from(parsed).map_err(|_| anyhow!("value does not fit command byte"))?;
        let index = match mask {
            1 => 2,
            2 => 3,
            4 => 4,
            8 => 5,
            16 => 6,
            _ => return Err(anyhow!("unsupported command mask {mask}")),
        };
        payload[index] = byte;
        let key = topic.to_string();
        if !self.pending.contains_key(&key) {
            self.order.push_back(key.clone());
        }
        self.pending
            .insert(key, EncodedCommand { raw_topic, payload });
        Ok(())
    }

    pub(super) fn observe_state(&mut self, state: &Map<String, Value>) {
        let Some(counter) = state.get(RECEIVE_COUNTER).and_then(Value::as_i64) else {
            return;
        };
        let previous_counter = self.counter.replace(counter);
        if previous_counter.is_some_and(|previous| counter < previous) {
            self.pending.clear();
            self.order.clear();
            self.in_flight_after = None;
            return;
        }
        let command_pending = state
            .get(COMMAND_PENDING)
            .and_then(Value::as_bool)
            .unwrap_or(true);
        if self
            .in_flight_after
            .is_some_and(|previous| counter > previous && !command_pending)
        {
            self.in_flight_after = None;
        }
    }

    pub(super) fn take_ready(&mut self) -> Option<EncodedCommand> {
        if self.in_flight_after.is_some() {
            return None;
        }
        let counter = self.counter?;
        while let Some(key) = self.order.pop_front() {
            if let Some(command) = self.pending.remove(&key) {
                self.in_flight_after = Some(counter);
                return Some(command);
            }
        }
        None
    }

    pub(super) fn reset_connection(&mut self) {
        self.pending.clear();
        self.order.clear();
        self.in_flight_after = None;
        self.counter = None;
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value};

    use super::*;
    use crate::layout::load_layout;

    fn state(counter: i64, pending: bool) -> Map<String, Value> {
        [
            (RECEIVE_COUNTER.to_string(), Value::from(counter)),
            (COMMAND_PENDING.to_string(), Value::from(pending)),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn validates_and_encodes_one_field_commands() {
        let layout = load_layout().unwrap();
        let mut queue = CommandQueue::default();
        queue.observe_state(&state(12, false));
        queue
            .enqueue(&layout, "reef/plc/command/gmp40_1/flow", b"73")
            .unwrap();

        assert_eq!(
            queue.take_ready().unwrap(),
            EncodedCommand {
                raw_topic: "plc/aquarium/command/gmp40_1".to_string(),
                payload: [1, 4, 0, 0, 73, 0, 0],
            }
        );
        assert!(queue.take_ready().is_none());
        queue.observe_state(&state(13, false));
    }

    #[test]
    fn coalesces_pending_values_and_never_replays_after_reset() {
        let layout = load_layout().unwrap();
        let mut queue = CommandQueue::default();
        queue.observe_state(&state(4, false));
        queue
            .enqueue(&layout, "reef/plc/command/gmp40_1/power", b"ON")
            .unwrap();
        assert!(queue.take_ready().is_some());
        queue
            .enqueue(&layout, "reef/plc/command/gmp40_1/flow", b"20")
            .unwrap();
        queue
            .enqueue(&layout, "reef/plc/command/gmp40_1/flow", b"21")
            .unwrap();
        assert!(queue.take_ready().is_none());
        queue.observe_state(&state(5, true));
        assert!(queue.take_ready().is_none());
        queue.observe_state(&state(5, false));
        assert_eq!(queue.take_ready().unwrap().payload, [1, 4, 0, 0, 21, 0, 0]);
        queue.reset_connection();
        queue.observe_state(&state(5, false));
        assert!(queue.take_ready().is_none());
    }

    #[test]
    fn rejects_out_of_range_and_malformed_commands() {
        let layout = load_layout().unwrap();
        let mut queue = CommandQueue::default();
        assert!(queue
            .enqueue(&layout, "reef/plc/command/gmp40_1/mode", b"9")
            .is_err());
        assert!(queue
            .enqueue(&layout, "reef/plc/command/gmp40_1/feed_time", b"0")
            .is_err());
        assert!(queue
            .enqueue(&layout, "reef/plc/command/gmp40_1/power", b"maybe")
            .is_err());
    }
}
