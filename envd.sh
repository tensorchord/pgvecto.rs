#!/usr/bin/bash

sudo apt-get update
sudo apt-get install -y lsb-release
sudo apt-get install -y gnupg
echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee -a /etc/apt/sources.list.d/pgdg.list
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC sudo -E apt-get install tzdata
sudo apt-get install -y build-essential
sudo apt-get install -y libpq-dev
sudo apt-get install -y libssl-dev
sudo apt-get install -y pkg-config
sudo apt-get install -y gcc
sudo apt-get install -y libreadline-dev
sudo apt-get install -y flex
sudo apt-get install -y bison
sudo apt-get install -y libxml2-dev
sudo apt-get install -y libxslt-dev
sudo apt-get install -y libxml2-utils
sudo apt-get install -y xsltproc
sudo apt-get install -y zlib1g-dev
sudo apt-get install -y ccache
sudo apt-get install -y clang
sudo apt-get install -y git
sudo apt-get install -y postgresql-15
sudo apt-get install -y postgresql-server-dev-15
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
rev=$(cat Cargo.toml | grep "pgrx =" | awk -F 'rev = "' '{print $2}' | cut -d'"' -f1)
cargo install cargo-pgrx --git https://github.com/tensorchord/pgrx.git --rev $rev
cargo pgrx init --pg15=/usr/lib/postgresql/15/bin/pg_config
sudo chmod 777 /usr/share/postgresql/15/extension/
sudo chmod 777 /usr/lib/postgresql/15/lib/
