FROM rust:1.95-slim-bookworm AS build

WORKDIR /build

COPY rust-toolchain.toml ./
RUN rustup show active-toolchain || rustup toolchain install

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release --locked --bin sealmesh-server

# The runtime image has no shell to create directories with, so the history
# directory is made here and copied over owned by the nonroot user (65532).
RUN mkdir -p /data

FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=build /build/target/release/sealmesh-server /usr/local/bin/sealmesh-server
COPY --from=build --chown=65532:65532 /data /data

EXPOSE 8787
VOLUME ["/data"]
ENV SEALMESH_LISTEN=0.0.0.0:8787
ENV SEALMESH_HISTORY_PATH=/data/history.sqlite

HEALTHCHECK --interval=30s --timeout=5s CMD ["/usr/local/bin/sealmesh-server", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/sealmesh-server"]
