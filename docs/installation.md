# Installation

## Try with docker

We have prebuild image at [tensorchord/pgvecto-rs](https://hub.docker.com/r/tensorchord/pgvecto-rs). You can try it with

```
docker run --name pgvecto-rs-demo -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d tensorchord/pgvecto-rs:pg16-v0.1.13
```

Connect to the database and enable the extension.

```sql
DROP EXTENSION IF EXISTS vectors;
CREATE EXTENSION vectors;
```

To acheive full performance, please mount the volume to pg data directory by adding the option like `-v $PWD/pgdata:/var/lib/postgresql/data`

You can configure PostgreSQL by the reference of the parent image in https://hub.docker.com/_/postgres/.

## Install from source

Install base dependency.

```sh
sudo apt install -y \
    build-essential \
    libpq-dev \
    libssl-dev \
    pkg-config \
    gcc \
    libreadline-dev \
    flex \
    bison \
    libxml2-dev \
    libxslt-dev \
    libxml2-utils \
    xsltproc \
    zlib1g-dev \
    ccache \
    clang \
    git
```

Install Rust. The following command will install Rustup, the Rust toolchain installer for your user. Do not install rustc using package manager.

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Install PostgreSQL and its headers. We assume you may install PostgreSQL 15. Feel free to replace `15` to any other major version number you need.

```sh
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" >> /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get -y install libpq-dev postgresql-15 postgresql-server-dev-15
```

Install clang-16. We do not support other versions of clang.

```sh
sudo sh -c 'echo "deb http://apt.llvm.org/$(lsb_release -cs)/ llvm-toolchain-$(lsb_release -cs)-16 main" >> /etc/apt/sources.list'
wget --quiet -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
sudo apt-get update
sudo apt-get -y install clang-16
```

Clone the Repository. Note the following commands are executed in the cloned repository directory.

```sh
git clone https://github.com/tensorchord/pgvecto.rs.git
cd pgvecto.rs
```

Install cargo-pgrx.

```sh
cargo install cargo-pgrx@$(grep 'pgrx = {' Cargo.toml | cut -d '"' -f 2)
cargo pgrx init --pg15=/usr/lib/postgresql/15/bin/pg_config
```

Install pgvecto.rs.

```sh
cargo pgrx install --sudo --release
```

Configure your PostgreSQL by modifying the `shared_preload_libraries` to include `vectors.so`.

```sh
psql -U postgres -c 'ALTER SYSTEM SET shared_preload_libraries = "vectors.so"'
```

You need restart the PostgreSQL cluster.

```sh
sudo systemctl restart postgresql.service
```

Connect to the database and enable the extension.

```sql
DROP EXTENSION IF EXISTS vectors;
CREATE EXTENSION vectors;
```

### Cross compilation

Assuming that you build target for aarch64 in a x86_64 host environment, you need to set right linker and sysroot for Rust.

```sh
sudo apt install crossbuild-essential-arm64
```

Add the following section to the end of `~/.cargo/config.toml`.

```toml
[target.aarch64-unknown-linux-gnu]
linker = "aarch64-linux-gnu-gcc"

[env]
BINDGEN_EXTRA_CLANG_ARGS_aarch64_unknown_linux_gnu = "-isystem /usr/aarch64-linux-gnu/include/ -ccc-gcc-name aarch64-linux-gnu-gcc"
```

## Install from release

Download the deb package in the release page, and type `sudo apt install vectors-pg15-*.deb` to install the deb package.

Configure your PostgreSQL by modifying the `shared_preload_libraries` to include `vectors.so`.

```sh
psql -U postgres -c 'ALTER SYSTEM SET shared_preload_libraries = "vectors.so"'
```

You need restart the PostgreSQL cluster.

```sh
sudo systemctl restart postgresql.service
```

Connect to the database and enable the extension.

```sql
DROP EXTENSION IF EXISTS vectors;
CREATE EXTENSION vectors;
```
