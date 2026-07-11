FROM rust:1.90-slim-trixie AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:trixie-slim
COPY --from=builder /app/target/release/prequal-rs /usr/local/bin/prequal-rs
ENV RUST_LOG=info
ENTRYPOINT ["prequal-rs"]
