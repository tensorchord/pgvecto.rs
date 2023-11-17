#!/usr/bin/env bash
set -e

if [ "$OS" == "ubuntu-latest" ]; then
    if [ $VERSION != 14 ]; then
        sudo pg_dropcluster 14 main
    fi
    sudo apt-get remove -y '^postgres.*' '^libpq.*' '^clang.*' '^llvm.*' '^libclang.*' '^libllvm.*' '^mono-llvm.*'
    sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
    sudo apt-get update
    sudo apt-get -y install build-essential libpq-dev postgresql-$VERSION postgresql-server-dev-$VERSION
    echo "local all all trust" | sudo tee /etc/postgresql/$VERSION/main/pg_hba.conf
    echo "host all all 127.0.0.1/32 trust" | sudo tee -a /etc/postgresql/$VERSION/main/pg_hba.conf
    echo "host all all ::1/128 trust" | sudo tee -a /etc/postgresql/$VERSION/main/pg_hba.conf
    pg_lsclusters
    sudo systemctl restart postgresql
    pg_lsclusters
    sudo -iu postgres createuser -s -r runner
    createdb
fi
if [ "$OS" == "macos-latest" ]; then
    brew uninstall postgresql
    brew install postgresql@$VERSION
    export PATH="$PATH:$(brew --prefix postgresql@$VERSION)/bin"
    echo "$(brew --prefix postgresql@$VERSION)/bin" >> $GITHUB_PATH
    brew services start postgresql@$VERSION
    sleep 5
    createdb
fi

sudo chmod -R 777 `pg_config --pkglibdir`
sudo chmod -R 777 `pg_config --sharedir`/extension

curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
cargo binstall sqllogictest-bin -y --force

cargo install cargo-pgrx --version $(grep '^pgrx ' Cargo.toml | awk -F'\"' '{print $2}') --debug
cargo pgrx init --pg$VERSION=$(which pg_config)
