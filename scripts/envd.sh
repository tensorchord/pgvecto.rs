#!/usr/bin/bash
set -e

echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee -a /etc/apt/sources.list.d/pgdg.list
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get install -y --no-install-recommends postgresql-15 postgresql-server-dev-15
sudo chmod 777 /usr/share/postgresql/15/extension/
sudo chmod 777 /usr/lib/postgresql/15/lib/

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none
cargo install cargo-pgrx@$(grep 'pgrx = {' Cargo.toml | cut -d '"' -f 2)
cargo pgrx init --pg15=/usr/lib/postgresql/15/bin/pg_config
