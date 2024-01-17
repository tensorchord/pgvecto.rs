#!/usr/bin/env bash
set -e

sqllogictest -u runner -d runner $(dirname $0)/*.slt
