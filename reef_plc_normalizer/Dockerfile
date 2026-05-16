FROM rust:1.85-alpine AS builder

WORKDIR /usr/src/reef-plc-normalizer
RUN apk add --no-cache musl-dev
COPY app/Cargo.toml app/Cargo.lock ./
COPY app/packed_mqtt_layout.yaml ./
COPY app/src ./src
RUN cargo build --release --locked

FROM ghcr.io/home-assistant/base:latest

ARG BUILD_VERSION=0.0.0-dev
ARG BUILD_ARCH=amd64

LABEL \
  io.hass.name="Reef PLC Normalizer" \
  io.hass.description="Normalize AutomationDirect P1-550 packed MQTT payloads for Home Assistant" \
  io.hass.version="${BUILD_VERSION}" \
  io.hass.type="app" \
  io.hass.arch="${BUILD_ARCH}"

COPY --from=builder /usr/src/reef-plc-normalizer/target/release/reef-plc-normalizer /usr/bin/reef-plc-normalizer
COPY run.sh /run.sh

RUN chmod a+x /run.sh

CMD ["/run.sh"]
