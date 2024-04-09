#!/usr/bin/env bash
set -e

# Test the background threads `optimizing.optimizing_indexing` and `optimizing.sealing_*` working properly
sqllogictest -u runner -d runner $(dirname $0)/create.slt
sleep 20
sqllogictest -u runner -d runner $(dirname $0)/check.slt
