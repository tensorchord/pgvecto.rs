#!/usr/bin/env bash
set -e

printf "VERSION = ${VERSION}\n"
printf "PGRX = ${PGRX}\n"

apt-get update
apt-get install -y --no-install-recommends ca-certificates curl build-essential gnupg lsb-release wget

echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | tee -a /etc/apt/sources.list.d/pgdg.list
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
apt-get update
apt-get install -y --no-install-recommends postgresql-${VERSION} postgresql-server-dev-${VERSION}

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env

cd $(mktemp -d)

cargo init --lib --name vectors
cargo add pgrx-pg-sys@=$PGRX --no-default-features --features pg$VERSION
PGRX_PG_CONFIG_PATH=$(which pg_config) PGRX_PG_SYS_EXTRA_OUTPUT_PATH=$(pwd)/pgrx-binding.rs cargo build
rustfmt ./pgrx-binding.rs

cp ./pgrx-binding.rs /mnt/build/vendor/pgrx_binding/pg${VERSION}_$(uname --machine)-unknown-linux-gnu.rs
pg_config > /mnt/build/vendor/pg_config/pg${VERSION}_$(uname --machine)-unknown-linux-gnu.txt
