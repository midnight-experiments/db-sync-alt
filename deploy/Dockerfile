# db-sync-alt container image: the release binary on a base with glibc and CA
# certificates and nothing else (no shell, no package manager, no PostgreSQL
# client tools). Build from the repository root:
#   docker build -f deploy/Dockerfile -t db-sync-alt:local .

FROM rust:1.100.0-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY tools ./tools
RUN cargo build --release --locked -p db-sync-alt \
    && install -D -m 0755 target/release/db-sync-alt /out/usr/local/bin/db-sync-alt \
    && install -d -m 0755 /out/var/lib/db-sync-alt/snapshots

# Debian 12 (bookworm), the same glibc as the build stage.
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
LABEL org.opencontainers.image.title="db-sync-alt" \
      org.opencontainers.image.description="Cardano data for Midnight nodes" \
      org.opencontainers.image.source="https://github.com/midnight-experiments/db-sync-alt" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /out/usr/local/bin/db-sync-alt /usr/local/bin/db-sync-alt
COPY --from=build --chown=65532:65532 /out/var/lib/db-sync-alt /var/lib/db-sync-alt
COPY LICENSE NOTICE THIRD_PARTY_NOTICES.md THIRD_PARTY_LICENSES.txt /usr/share/doc/db-sync-alt/
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/db-sync-alt"]
CMD ["run"]
