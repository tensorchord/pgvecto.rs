#!/usr/bin/env bash
set -e

printf "SEMVER = ${SEMVER}\n"
printf "VERSION = ${VERSION}\n"
printf "ARCH = ${ARCH}\n"

export PLATFORM=$(echo $ARCH | sed 's/aarch64/arm64/; s/x86_64/amd64/')

cargo build --release --no-default-features --features pg$VERSION --target ${ARCH}-unknown-linux-gnu
cargo pgrx schema --no-default-features --features pg$VERSION | expand -t 4 > ./target/vectors--$SEMVER.sql

rm -rf ./build/dir_zip
rm -rf ./build/vectors-pg${VERSION}_${ARCH}-unknown-linux-gnu_${SEMVER}.zip
rm -rf ./build/dir_deb
rm -rf ./build/vectors-pg${VERSION}_${SEMVER}-1_${PLATFORM}.deb

mkdir -p ./build/dir_zip
cp -a ./sql/upgrade/. ./build/dir_zip/
cp ./target/vectors--$SEMVER.sql ./build/dir_zip/vectors--$SEMVER.sql
sed -e "s/@CARGO_VERSION@/$SEMVER/g" < ./vectors.control > ./build/dir_zip/vectors.control
cp ./target/${ARCH}-unknown-linux-gnu/release/libvectors.so ./build/dir_zip/vectors.so
zip ./build/vectors-pg${VERSION}_${ARCH}-unknown-linux-gnu_${SEMVER}.zip -j ./build/dir_zip/*

mkdir -p ./build/dir_deb
mkdir -p ./build/dir_deb/DEBIAN
mkdir -p ./build/dir_deb/usr/share/postgresql/$VERSION/extension/
mkdir -p ./build/dir_deb/usr/lib/postgresql/$VERSION/lib/
for file in $(ls ./build/dir_zip/*.sql | xargs -n 1 basename); do
    cp ./build/dir_zip/$file ./build/dir_deb/usr/share/postgresql/$VERSION/extension/$file
done
for file in $(ls ./build/dir_zip/*.control | xargs -n 1 basename); do
    cp ./build/dir_zip/$file ./build/dir_deb/usr/share/postgresql/$VERSION/extension/$file
done
for file in $(ls ./build/dir_zip/*.so | xargs -n 1 basename); do
    cp ./build/dir_zip/$file ./build/dir_deb/usr/lib/postgresql/$VERSION/lib/$file
done
echo "Package: vectors-pg${VERSION}
Version: ${SEMVER}
Section: database
Priority: optional
Architecture: ${PLATFORM}
Maintainer: Tensorchord <support@tensorchord.ai>
Description: Vector database plugin for Postgres, written in Rust, specifically designed for LLM
Homepage: https://pgvecto.rs/
License: apache2" \
> ./build/dir_deb/DEBIAN/control
(cd ./build/dir_deb && md5sum usr/share/postgresql/$VERSION/extension/* usr/lib/postgresql/$VERSION/lib/*) > ./build/dir_deb/DEBIAN/md5sums
dpkg --build ./build/dir_deb/ ./build/vectors-pg${VERSION}_${SEMVER}-1_${PLATFORM}.deb
