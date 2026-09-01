# syntax=docker/dockerfile:1.4
FROM rust:1.98.0-slim-trixie@sha256:fb4b2f1dc68c06f46618948b09d0ade147e6d2b11a6581e599b0c808d5b8a167 AS rust-builder

WORKDIR /usr/src/app

ENV SQLX_OFFLINE=true

RUN --mount=type=bind,source=.sqlx,target=.sqlx \
    --mount=type=bind,source=migrations,target=migrations \
    --mount=type=bind,source=src,target=src \
    --mount=type=bind,source=Cargo.toml,target=Cargo.toml \
    --mount=type=bind,source=Cargo.lock,target=Cargo.lock \
    --mount=type=cache,target=/usr/src/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo build --locked --release && cp target/release/om26_18 /tmp/app

FROM ghcr.io/denoland/deno:2.9.5@sha256:b429777c3dcff34a6488f365a1537db1640b2d48379b60f5e6206be034472463 AS deno-builder

WORKDIR /app

RUN --mount=type=bind,source=deno.jsonc,target=deno.jsonc \
    --mount=type=bind,source=deno.lock,target=deno.lock \
    --mount=type=cache,target=/deno-dir/ \
    deno install --frozen

COPY deno.jsonc deno.lock .
COPY api/om26_18.schemas.ts api/
COPY src/services/textalive.ts src/services/

RUN --mount=type=cache,target=/deno-dir/ \
    deno task bundle

FROM ghcr.io/denoland/deno:distroless-2.9.5@sha256:1bc3ce768279a9fb68e289916d8c33d6d10e002c18ddaf57f62a35daca0e5691

COPY --from=rust-builder /tmp/app /usr/local/bin/app
COPY --from=deno-builder /app/dist/textalive.js dist/textalive.js

EXPOSE 8080
EXPOSE 4433/udp

ENTRYPOINT ["/usr/local/bin/app"]
