#!/usr/bin/bash

set -e

apt-get update
apt-get install -y wget
apt-get install -y curl

if [ "$TAG" == "latest" ]; then
    URL=https://api.github.com/repos/tensorchord/pgvecto.rs/releases/latest
else
    URL=https://api.github.com/repos/tensorchord/pgvecto.rs/releases/tags/$TAG
fi

DOWNLOAD=$(curl -s $URL | grep browser_download_url | grep -o 'https://[^ ]*vectors-pg15-[^ ]*amd64-unknown-linux-gnu\.deb')

wget -O /tmp/vectors.deb $DOWNLOAD
apt-get install -y /tmp/vectors.deb
rm -f /tmp/vectors.deb
