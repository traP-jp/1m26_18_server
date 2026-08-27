# syntax=docker/dockerfile:1.4
FROM rust:1.98.0-slim-trixie@sha256:fb4b2f1dc68c06f46618948b09d0ade147e6d2b11a6581e599b0c808d5b8a167 AS builder

WORKDIR /usr/src/app

RUN --mount=type=bind,source=src,target=src \
    --mount=type=bind,source=migrations,target=migrations \
    --mount=type=bind,source=Cargo.toml,target=Cargo.toml \
    --mount=type=bind,source=Cargo.lock,target=Cargo.lock \
    --mount=type=cache,target=/usr/src/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo build --locked --release && cp target/release/app /tmp/app

FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder /tmp/app /usr/local/bin/app

EXPOSE 8080

CMD ["/usr/local/bin/app"]
