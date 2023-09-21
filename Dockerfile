FROM rust:bookworm as builder
WORKDIR /workdir
COPY . .
RUN cargo build --target x86_64-unknown-linux-gnu --release


FROM rust:slim-bookworm
WORKDIR /home/rl-stats-ingester
COPY --from=builder /workdir/target/x86_64-unknown-linux-gnu/release/rl-stats-ingester /home/rl-stats-ingester/

# Avoid running the connector as root.
#USER nonroot:nonroot

ENTRYPOINT ["/home/rl-stats-ingester/rl-stats-ingester"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL FLOW_RUNTIME_CODEC=json
LABEL CONNECTOR_PROTOCOL=flow-capture

