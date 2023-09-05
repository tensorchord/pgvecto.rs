FROM scratch
COPY ./pgvecto-rs-binary-release.deb /
CMD ["/pgvecto-rs-binary-release.deb"]
