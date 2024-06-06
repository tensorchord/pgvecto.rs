ARG FROM_TAG
ARG CNPG_TAG
FROM scratch as nothing
ARG TARGETARCH
FROM tensorchord/pgvecto-rs-binary:${FROM_TAG}-${TARGETARCH} as binary

FROM ghcr.io/cloudnative-pg/postgresql:$CNPG_TAG
# use root to install the extension
USER root
COPY --from=binary ./pgvecto-rs-binary-release.deb /tmp/vectors.deb
RUN apt install -y /tmp/vectors.deb && rm -f /tmp/vectors.deb

USER postgres
