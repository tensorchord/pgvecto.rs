#!/usr/bin/env bash
set -e

psql -f $(dirname $0)/init.sql

for x in $(dirname $0)/*/test.sh; do
    $x
done
