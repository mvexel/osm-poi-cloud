# OSM-H3 Sharder
# Rust-based H3 sharding for planet files

FROM rust:1.92-bullseye AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy src for dependency caching
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies only
RUN cargo build --release && rm -rf src

# Copy actual source
COPY src ./src

# Build the real binary (touch to ensure rebuild)
RUN touch src/main.rs && cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    aria2 \
    awscli \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/osm-planet-sharding /usr/local/bin/

# Create working directory
WORKDIR /data

# Entrypoint script that handles downloading and sharding
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
