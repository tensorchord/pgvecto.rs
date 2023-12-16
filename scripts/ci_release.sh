#!/usr/bin/env bash
set -e

sed -i "s/@CARGO_VERSION@/${SEMVER}/g" ./vectors.control

git add -A
git commit -m "chore: release"
git tag v$SEMVER
git push origin v$SEMVER
