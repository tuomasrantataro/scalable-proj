FROM rust:1.98.1-slim-bookworm AS build
WORKDIR /build
# Build dependencies first, with a stub main, so source changes do not rebuild the whole tree.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release --locked && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /build/target/release/replay-producer /app/replay-producer
ENTRYPOINT ["/app/replay-producer"]
CMD ["replay"]
