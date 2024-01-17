#!/usr/bin/env bash
set -e

sudo pip install psutil==5.9.7
sqllogictest -u runner -d runner $(dirname $0)/create.slt
sudo python $(dirname $0)/kill.py
sqllogictest -u runner -d runner $(dirname $0)/restore.slt
