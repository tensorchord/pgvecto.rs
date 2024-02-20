#!/usr/bin/env bash
set -e

cargo pgrx install --no-default-features --features "pg$VERSION" --release
psql -c 'ALTER SYSTEM SET shared_preload_libraries = "vectors.so"'
psql -c 'ALTER SYSTEM SET search_path = "$user", public, vectors'
psql -c 'ALTER SYSTEM SET logging_collector = on'

if [ "$OS" == "ubuntu-latest" ]; then
    sudo systemctl restart postgresql
    pg_lsclusters
fi
