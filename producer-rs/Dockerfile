FROM rust:1.98.1-slim-bookworm AS build
# librdkafka is built from source by the rdkafka crate (cmake-build feature). librdkafka 2.12 includes
# curl/curl.h even when built without curl, so the headers are needed (nothing is linked against libcurl).
RUN apt-get update && apt-get install -y --no-install-recommends cmake g++ make libcurl4-openssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /build
# Build dependencies first, with a stub main, so source changes do not rebuild the whole tree.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release --locked && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo test --release --locked && cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /build/target/release/replay-producer /app/replay-producer
ENTRYPOINT ["/app/replay-producer"]
CMD ["replay"]
