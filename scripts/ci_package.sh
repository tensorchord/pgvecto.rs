#!/usr/bin/env bash
set -e

cargo pgrx package

for file in ./sql/upgrade/*; do
    cp "$file" "./target/release/vectors-pg$VERSION/usr/share/postgresql/$VERSION/extension/$(basename "$file")"
done
