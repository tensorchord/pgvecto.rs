#!/usr/bin/env bash
set -e

printf "VERSION = ${VERSION}\n"
printf "_PGRX = ${_PGRX}\n"
printf "_RUST = ${_RUST}\n"

if ! command -v rustup &> /dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly
    source "$HOME/.cargo/env"
fi
rustup toolchain install $_RUST

cargo +$_RUST install cargo-pgrx@$_PGRX --debug
cargo pgrx init --pg${VERSION}=/usr/lib/postgresql/${VERSION}/bin/pg_config
