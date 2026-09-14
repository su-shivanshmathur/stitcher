# Stage 0 — build
FROM rust:1-bookworm AS builder

# native deps: librdkafka (cmake-build, SASL), RocksDB C++ (clang), OpenSSL (ssl)
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake clang libsasl2-dev libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY config ./config

# --locked: the git revs + serde pin live in Cargo.lock
RUN cargo build --locked --release -p stitcher --bin stitcher

# Stage 1 — runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libsasl2-2 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 stitcher
USER stitcher
WORKDIR /app

COPY --from=builder /src/target/release/stitcher /app/
COPY --from=builder /src/config /app/config

# Production config is injected at runtime: STITCHER__* env vars, or a mounted
# file passed via --config-path. Missing config fails fast at validation.
ENV RUN_ENV=production CONFIG_DIR=/app/config
EXPOSE 9090
ENTRYPOINT ["/app/stitcher"]
