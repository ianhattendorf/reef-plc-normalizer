use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::layout::Layout;

const CACHED_STATE_REPLAY_MAX_AGE_SECONDS: u64 = 60;

#[derive(Debug, Clone)]
pub(super) struct CachedState {
    pub(super) payload: String,
    pub(super) updated_at: Instant,
}

#[derive(Debug)]
pub(super) struct ReconnectBackoff {
    initial: Duration,
    current: Duration,
    max: Duration,
}

impl ReconnectBackoff {
    pub(super) fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            current: initial,
            max,
        }
    }

    pub(super) fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = self.current.saturating_mul(2).min(self.max);
        delay
    }

    pub(super) fn reset(&mut self) {
        self.current = self.initial;
    }
}

pub(super) fn fresh_cached_states<'a>(
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
