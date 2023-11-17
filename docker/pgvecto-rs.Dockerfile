ARG TAG
ARG POSTGRES_VERSION
ARG IMAGEPATH=tensorchord/pgvecto-rs-binary:$TAG
FROM $IMAGEPATH as binary
FROM postgres:$POSTGRES_VERSION

COPY --from=binary /pgvecto-rs-binary-release.deb /tmp/vectors.deb
RUN apt-get install -y /tmp/vectors.deb && rm -f /tmp/vectors.deb
CMD ["postgres","-c","shared_preload_libraries=vectors.so"]
