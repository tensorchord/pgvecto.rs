#!/usr/bin/env bash
set -e

if [ "$OS" == "ubuntu-latest" ]; then
    if [ $VERSION != 14 ]; then
        sudo pg_dropcluster 14 main
    fi
    sudo apt-get remove -y '^postgres.*' '^libpq.*' '^clang.*' '^llvm.*' '^libclang.*' '^libllvm.*' '^mono-llvm.*'
    sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" >> /etc/apt/sources.list.d/pgdg.list'
    sudo sh -c 'echo "deb http://apt.llvm.org/$(lsb_release -cs)/ llvm-toolchain-$(lsb_release -cs)-16 main" >> /etc/apt/sources.list'
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
    wget --quiet -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
    sudo apt-get update
    sudo apt-get -y install build-essential libpq-dev postgresql-$VERSION postgresql-server-dev-$VERSION
    sudo apt-get -y install clang-16
    sudo apt-get -y install crossbuild-essential-arm64
    echo 'target.aarch64-unknown-linux-gnu.linker = "aarch64-linux-gnu-gcc"' | tee ~/.cargo/config.toml
    echo 'env.BINDGEN_EXTRA_CLANG_ARGS_aarch64_unknown_linux_gnu = "-isystem /usr/aarch64-linux-gnu/include/ -ccc-gcc-name aarch64-linux-gnu-gcc"' | tee -a ~/.cargo/config.toml
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
    sleep 30
    createdb
fi

sudo chmod -R 777 `pg_config --pkglibdir`
sudo chmod -R 777 `pg_config --sharedir`/extension

curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
cargo binstall sqllogictest-bin -y --force

cargo install cargo-pgrx@$(grep 'pgrx = {' Cargo.toml | cut -d '"' -f 2) --debug
cargo pgrx init --pg$VERSION=$(which pg_config)
