# Repository Guidelines

## Project Structure & Module Organization

This is a Home Assistant app repository. Root metadata is in `repository.yaml`;
the app lives under `reef_plc_normalizer/`:

- `config.yaml`: Home Assistant app metadata and options.
- `Dockerfile` and `run.sh`: container build and startup.
- `app/`: Rust implementation, with source in `app/src/main.rs`.
- `app/packed_mqtt_layout.yaml`: embedded PLC field map and MQTT discovery data.
- `README.md`, `DOCS.md`, and `CHANGELOG.md`: app docs and concise changes.
- `scripts/`: release validation and version bump helpers.

Rust unit tests currently live inline in `app/src/main.rs`.

## Build, Test, and Development Commands

Run commands from the repository root unless noted:

- `cargo fmt --manifest-path reef_plc_normalizer/app/Cargo.toml -- --check`:
  check Rust formatting.
- `cargo test --manifest-path reef_plc_normalizer/app/Cargo.toml --locked`:
  run tests using the committed lockfile.
- `scripts/check-release.sh`: verify release metadata is synchronized.
- `scripts/release.sh patch --note "Describe the change"`: run the full release
  flow; also accepts `minor`, `major`, or an explicit version like `0.2.0`.

## Coding Style & Naming Conventions

Use standard Rust formatting via `cargo fmt`; keep code compatible with the
edition in `app/Cargo.toml`. Use snake_case for Rust functions, variables, and
tests. Keep MQTT topics and Home Assistant entity identifiers stable unless the
change is intentional and documented.

Use YAML for Home Assistant and PLC layout data. Keep each layout entry explicit:
source name, packed length, value type, and discovery metadata.

## Testing Guidelines

Add focused unit tests for parsing, normalization, validation, and discovery
payload behavior. Existing tests use descriptive names such as
`rejects_short_payloads` and `discovery_omits_ai_by_default`; follow that style.
Run formatting and tests before committing.

## Commit & Pull Request Guidelines

Recent commits use short imperative subjects, for example
`Add release preparation tooling` and `Release reef PLC normalizer 0.1.1`.
Keep commits scoped and avoid mixing release bumps with unrelated code changes.

Pull requests should include a brief summary, validation commands run, and any
Home Assistant behavior or MQTT topic changes. Update `CHANGELOG.md` with a
concise note describing user-visible or operational changes made.

## Release Notes

Home Assistant update detection depends on `reef_plc_normalizer/config.yaml`
`version`. Use `scripts/release.sh` to update release metadata, commit, tag, and
push. Release images are published by CI from tags like `v0.1.1`; do not publish
release images from an untagged local build.
