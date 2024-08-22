#!/usr/bin/env bash
set -e

printf "VERSION = ${VERSION}\n"
printf "BRANCH = ${BRANCH}\n"

apt-get update
apt-get install -y --no-install-recommends ca-certificates curl build-essential gnupg lsb-release wget git

echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | tee -a /etc/apt/sources.list.d/pgdg.list
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
apt-get update
apt-get install -y --no-install-recommends postgresql-${VERSION} postgresql-server-dev-${VERSION}

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env

cd $(mktemp -d)

cargo init --lib --name vectors
cargo add pgrx-pg-sys --git https://github.com/tensorchord/pgrx.git --branch $BRANCH --no-default-features --features pg$VERSION
PGRX_PG_CONFIG_PATH=$(which pg_config) PGRX_PG_SYS_EXTRA_OUTPUT_PATH=$(pwd)/pgrx_binding.rs cargo build
rustfmt ./pgrx_binding.rs

mkdir -p /mnt/build/vendor/pg${VERSION}_$(uname --machine)_debian
mkdir -p /mnt/build/vendor/pg${VERSION}_$(uname --machine)_debian/pg_config
mkdir -p /mnt/build/vendor/pg${VERSION}_$(uname --machine)_debian/pgrx_binding

touch /mnt/build/vendor/pg${VERSION}_$(uname --machine)_debian/pg_config/pg_config
echo "#!/usr/bin/env bash" > /mnt/build/vendor/pg${VERSION}_$(uname --machine)_debian/pg_config/pg_config
echo '$(dirname "$0")/../../../tools/pg_config.sh "$@" < $(dirname "$0")/pg_config.txt' >> /mnt/build/vendor/pg${VERSION}_$(uname --machine)_debian/pg_config/pg_config
chmod 777 /mnt/build/vendor/pg${VERSION}_$(uname --machine)_debian/pg_config/pg_config
pg_config > /mnt/build/vendor/pg${VERSION}_$(uname --machine)_debian/pg_config/pg_config.txt

cp ./pgrx_binding.rs /mnt/build/vendor/pg${VERSION}_$(uname --machine)_debian/pgrx_binding/pg${VERSION}_raw_bindings.rs
