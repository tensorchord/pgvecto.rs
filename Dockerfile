FROM postgres:15

ARG TAG=latest

COPY . /tmp/build
RUN (cd /tmp/build && ./docker.sh)
