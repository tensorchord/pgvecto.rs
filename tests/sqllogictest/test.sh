#!/usr/bin/env bash
set -e

sqllogictest -d $USER $(dirname $0)/*.slt

if [ "$(psql -tAqX -c "SHOW server_version_num")" -ge 140000 ]; then
    sqllogictest -d $USER $(dirname $0)/pg14/*.slt
fi
