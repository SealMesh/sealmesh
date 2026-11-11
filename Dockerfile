FROM rust:1.99-slim-bookworm@sha256:f2c004e3d2d8b547e8cf7ffbc29205385f4e71d23e53e4e383065db5e9f822cd AS build

WORKDIR /build

COPY rust-toolchain.toml ./
RUN rustup show active-toolchain || rustup toolchain install

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Empty by default: the published `<version>` image keeps history in SQLite and
# carries no AWS SDK. `--build-arg FEATURES=dynamodb` builds the
# `<version>-dynamodb` variant the AWS recipe in deploy/aws runs.
ARG FEATURES=""
RUN cargo build --release --locked -p sealmesh-server --bin sealmesh-server \
    ${FEATURES:+--features "$FEATURES"}

# The runtime image has no shell to create directories with, so the history
# directory is made here and copied over owned by the nonroot user (65532).
RUN mkdir -p /data

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f

COPY --from=build /build/target/release/sealmesh-server /usr/local/bin/sealmesh-server
COPY --from=build --chown=65532:65532 /data /data

EXPOSE 8787
VOLUME ["/data"]
ENV SEALMESH_LISTEN=0.0.0.0:8787
ENV SEALMESH_HISTORY_PATH=/data/history.sqlite

HEALTHCHECK --interval=30s --timeout=5s CMD ["/usr/local/bin/sealmesh-server", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/sealmesh-server"]
