# Development

## Environment

You can setup development environment simply using `envd`. It will create a docker container and install all the dependencies for you.

```sh
pip install envd
git clone https://github.com/tensorchord/pgvecto.rs.git # or `git clone git@github.com:tensorchord/pgvecto.rs.git`
cd pgvecto.rs
envd up
```

Or you can setup development environment following these steps manually:

1. Install base dependency.

```sh
sudo apt install -y \
    bison \
    build-essential \
    ccache \
    flex \
    gcc \
    git \
    gnupg \
    libreadline-dev \
    libssl-dev \
    libxml2-dev \
    libxml2-utils \
    libxslt-dev \
    lsb-release \
    pkg-config \
    tzdata \
    xsltproc \
    zlib1g-dev
```

2. Clone the Repository.

```sh
git clone https://github.com/tensorchord/pgvecto.rs.git # or `git clone git@github.com:tensorchord/pgvecto.rs.git`
cd pgvecto.rs
```

3. Install PostgreSQL and its headers. We assume you may install PostgreSQL 15. Feel free to replace `15` to any other major version number you need.

```sh
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" >> /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get install -y --no-install-recommends libpq-dev postgresql-15 postgresql-server-dev-15
```

4. Install clang-16. We do not support other versions of clang.

```sh
sudo sh -c 'echo "deb http://apt.llvm.org/$(lsb_release -cs)/ llvm-toolchain-$(lsb_release -cs)-16 main" >> /etc/apt/sources.list'
wget --quiet -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
sudo apt-get update
sudo apt-get install -y --no-install-recommends clang-16
```

5. Install Rust. The following command will install Rustup, the Rust toolchain installer for your user. Do not install rustc using package manager.

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

6. Install cargo-pgrx.

```sh
cargo install cargo-pgrx@$(grep 'pgrx = {' Cargo.toml | cut -d '"' -f 2)
cargo pgrx init --pg15=/usr/lib/postgresql/15/bin/pg_config
```

7. The following command is helpful if you are struggling with permission issues.

```sh
sudo chmod 777 /usr/share/postgresql/15/extension/
sudo chmod 777 /usr/lib/postgresql/15/lib/
```

### Cross compilation

Assuming that you build target for aarch64 in a x86_64 host environment, you can follow these steps:

1. Install cross compilation toolchain.

```sh
sudo apt install crossbuild-essential-arm64
```

2. Get PostgreSQL header files on target architecture.

```sh
apt download postgresql-server-dev-15:arm64
```

3. Set right linker and sysroot for Rust by adding the following section to the end of `~/.cargo/config.toml`.

```toml
[target.aarch64-unknown-linux-gnu]
linker = "aarch64-linux-gnu-gcc"

[env]
BINDGEN_EXTRA_CLANG_ARGS_aarch64_unknown_linux_gnu = "-isystem /usr/aarch64-linux-gnu/include/ -ccc-gcc-name aarch64-linux-gnu-gcc"
```

## Debug

Debug information included in the compiled binary even in release mode so you can always use `gdb` for debugging.

For a debug build, backtrace is printed when a thread in background worker process panics, but not for a session process error. For a release build, backtrace is never printed. But if you set environment variable `RUST_BACKTRACE` to `1`, all backtraces are printed. It's recommended for you to debug a release build with the command `RUST_BACKTRACE=1 cargo pgrx run --release`.

## Pull request

### Version

pgvecto.rs uses `pg_vectors` directory under PostgreSQL data directory for storage. To reduce the unnecessary rebuilding indexes when upgrade, we record version number of persistent data. If you modify the structure of persistent data, you need to bump the `VERSION` (if it's a breaking change) or `SOFT_VERSION` (if a newer version can still read old data).

The version number is saved in these two files:

1. `/crates/service/src/worker/metadata.rs` (if the structure of persistent data you modified is outside an index).
2. `/crates/service/src/instance/metadata.rs` (if the structure of persistent data you modified is inside an index).

## Release

These steps are needed for a release:

1. Get a new version number. Let's say it's `99.99.99` and its former version number is `98.98.98`.
2. Push these changes to `main` branch.
    * Modify the latest version number in `/README.md` and `/docs/installation.md` to `99.99.99`.
    * Use `cargo pgrx schema` to generate a schema script and upload it to `/sql/vectors--99.99.99.sql`.
    * Write a schema update script and upload it to `/sql/vectors--98.98.98--99.99.99.sql`.
3. Manually trigger `Release` CI.

These steps are needed for a prerelease:

1. Get a new version number. Let's say it's `99.99.99-alpha`.
2. Manually trigger `Release` CI with checkbox `prerelease` checked.
