use std::fs;

use anyhow::{Context, Result};
use clap::Parser;
use serde::Deserialize;

#[derive(Debug, Parser)]
pub(super) struct Args {
    #[arg(long, default_value = "/data/options.json")]
    pub(super) options: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct AppOptions {
    pub(super) mqtt_host: String,
    pub(super) mqtt_port: u16,
    #[serde(default)]
    pub(super) mqtt_username: String,
    #[serde(default)]
    pub(super) mqtt_password: String,
    #[serde(default = "default_discovery_prefix")]
    pub(super) discovery_prefix: String,
    #[serde(default)]
    pub(super) publish_diagnostic_ai: bool,
    #[serde(default = "default_log_level")]
    pub(super) log_level: String,
}

fn default_discovery_prefix() -> String {
    "homeassistant".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}

pub(super) fn load_options(path: &str) -> Result<AppOptions> {
    let raw = fs::read_to_string(path).with_context(|| format!("failed to read {path}"))?;
    serde_json::from_str(&raw).with_context(|| format!("failed to parse {path}"))
}
