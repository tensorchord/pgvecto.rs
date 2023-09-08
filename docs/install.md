
# Installation

## Try with docker

We have prebuild image at [tensorchord/pgvecto-rs](https://hub.docker.com/r/tensorchord/pgvecto-rs). You can try it with

```
docker run --name pgvecto-rs-demo -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d tensorchord/pgvecto-rs:latest
```

To acheive full performance, please mount the volume to pg data directory by adding the option like `-v $PWD/pgdata:/var/lib/postgresql/data`

Reference: https://hub.docker.com/_/postgres/.

<details>
  <summary>Build from source</summary>

## Install Rust and base dependency

```sh
sudo apt install -y build-essential libpq-dev libssl-dev pkg-config gcc libreadline-dev flex bison libxml2-dev libxslt-dev libxml2-utils xsltproc zlib1g-dev ccache clang git
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Clone the Repository

```sh
git clone https://github.com/tensorchord/pgvecto.rs.git
cd pgvecto.rs
```

## Install Postgresql and pgrx

```sh
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get -y install libpq-dev postgresql-15 postgresql-server-dev-15
cargo install cargo-pgrx --git https://github.com/tensorchord/pgrx.git --rev $(cat Cargo.toml | grep "pgrx =" | awk -F'rev = "' '{print $2}' | cut -d'"' -f1)
cargo pgrx init --pg15=/usr/lib/postgresql/15/bin/pg_config
```

## Install pgvecto.rs

```sh
cargo pgrx install --release
```

Configure your PostgreSQL by modifying the `shared_preload_libraries` to include `vectors.so`.

```sh
psql -U postgres -c 'ALTER SYSTEM SET shared_preload_libraries = "vectors.so"'
```

You need restart the PostgreSQL cluster.

```sh
sudo systemctl restart postgresql.service
```

</details>

<details>
  <summary>Install from release</summary>

Download the deb package in the release page, and type `sudo apt install vectors-pg15-*.deb` to install the deb package.

Configure your PostgreSQL by modifying the `shared_preload_libraries` to include `vectors.so`.

```sh
psql -U postgres -c 'ALTER SYSTEM SET shared_preload_libraries = "vectors.so"'
```

You need restart the PostgreSQL cluster.

```sh
sudo systemctl restart postgresql.service
```

</details>

Connect to the database and enable the extension.

```sql
DROP EXTENSION IF EXISTS vectors;
CREATE EXTENSION vectors;
```
