ARG FROM_TAG
ARG CNPG_TAG
ARG SCHEMA
FROM scratch as nothing
ARG TARGETARCH
FROM modelzai/pgvecto-rs-binary:${FROM_TAG}-${TARGETARCH}-${SCHEMA} as binary

FROM ghcr.io/cloudnative-pg/postgresql:$CNPG_TAG-bookworm
# use root to install the extension
USER root
COPY --from=binary /pgvecto-rs-binary-release.deb /tmp/vectors.deb
RUN apt install -y /tmp/vectors.deb && rm -f /tmp/vectors.deb

USER postgres
