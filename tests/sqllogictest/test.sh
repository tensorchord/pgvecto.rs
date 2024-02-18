#!/usr/bin/env bash
set -e

sqllogictest -d $USER $(dirname $0)/*.slt
