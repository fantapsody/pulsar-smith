FROM rust:1.66.0 AS build
WORKDIR /usr/src

RUN rustup target add x86_64-unknown-linux-musl

RUN apt update && \
    apt install protobuf-compiler musl-tools -y && \
    apt clean

RUN USER=root cargo new pulsar-smith
WORKDIR /usr/src/pulsar-smith
COPY Cargo.toml Cargo.lock ./
RUN echo 'openssl = { version = "0.10", features = ["vendored"] }' >> Cargo.toml
RUN cargo build --release

COPY src ./src
RUN cargo install --target x86_64-unknown-linux-musl --path .

FROM alpine/k8s:1.23.16
COPY --from=build /usr/local/cargo/bin/pulsar-smith .
USER 0
CMD ["./pulsar-smith"]
