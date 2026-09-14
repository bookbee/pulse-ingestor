# pulse-ingestor container image.
#
# Multi-stage: build with the Rust toolchain, ship a slim Debian runtime.
#
# Debian rather than Alpine, unlike pulse-gateway: rdkafka compiles librdkafka
# from source, and doing that against musl buys nothing here while costing a
# more fragile build. The binary is therefore glibc-linked and needs a glibc
# runtime image.
#
# Build and run from the repo root — or just use the Makefile:
#   make image
#   make run-image
#
# Manually:
#   docker build -t pulse-ingestor:local .
#   docker run --rm --network pulse-infra \
#     --env-file .env \
#     -e KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092 \
#     -e STORAGE_EMULATOR_HOST=fake-gcs:4443 \
#     pulse-ingestor:local
#
# The two -e overrides matter: .env holds HOST addresses (localhost:19092),
# which resolve to the container itself once inside the pulse-infra network.

# ─── Build ───────────────────────────────────────────────────────────────────
FROM rust:1-bookworm AS build

WORKDIR /src

# Dependencies first, against dummy sources: this layer stays cached until
# Cargo.toml/Cargo.lock actually change, so ordinary source edits don't rebuild
# librdkafka, arrow and parquet from scratch (~90s of the build).
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
    && echo 'fn main() {}' > src/main.rs \
    && : > src/lib.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src

# Cargo keys off mtime; the dummy artifacts above would otherwise look current.
RUN touch src/main.rs src/lib.rs \
    && cargo build --release --locked \
    && strip target/release/pulse-ingestor

# ─── Runtime ─────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

# ca-certificates for TLS to real GCS. Nothing else is needed: librdkafka and
# zlib are linked in, and rustls carries its own TLS stack rather than OpenSSL.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin pulse

COPY --from=build /src/target/release/pulse-ingestor /usr/local/bin/pulse-ingestor

# dotenvy reads .env from the WORKING DIRECTORY when one is present. Nothing is
# baked in: in a container, config comes from the environment. A missing .env is
# not an error — Config::from_env reports any actually-missing variable by name.
WORKDIR /app

# Unprivileged: this service binds no ports and only makes outbound connections.
USER pulse

# No HEALTHCHECK and no EXPOSE on purpose. The ingestor serves no HTTP surface,
# so there is nothing to probe — "is the process alive" is what Docker already
# tracks, and a check that always passes is worse than none. Liveness is
# consumer lag, which belongs in monitoring rather than in a container probe.

ENTRYPOINT ["/usr/local/bin/pulse-ingestor"]
