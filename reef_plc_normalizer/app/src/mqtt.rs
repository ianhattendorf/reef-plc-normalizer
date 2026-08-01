use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, LastWill, MqttOptions, QoS};
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::availability::{fresh_cached_states, CachedState, ReconnectBackoff};
use crate::config::AppOptions;
use crate::discovery::publish_discovery;
use crate::layout::Layout;
use crate::normalize::normalize_payload;
use crate::AVAILABILITY_TOPIC;

const CLIENT_ID: &str = "reef-plc-normalizer";
const HA_STATUS_TOPIC: &str = "homeassistant/status";
const MQTT_REQUEST_CHANNEL_CAPACITY: usize = 256;
const MQTT_RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const MQTT_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);

pub(super) async fn run(options: AppOptions, layout: Layout) -> Result<()> {
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
