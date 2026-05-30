# ── Stage 1: Build Backend ───────────────────────────────────────────────────
FROM rust:1.75-slim as backend-builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy backend source
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests

# Build release binary
RUN cargo build --release

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=backend-builder /app/target/release/backend /app/backend

# Expose backend port
EXPOSE 3001

# Run
CMD ["/app/backend"]
