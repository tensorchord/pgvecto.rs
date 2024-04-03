#!/usr/bin/env bash
set -e

# Test the background threads `optimizing.indexing` and `optimizing.sealing` working properly
sqllogictest -u runner -d runner $(dirname $0)/create.slt
sleep 240
sqllogictest -u runner -d runner $(dirname $0)/check.slt
