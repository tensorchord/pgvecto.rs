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

To achieve full performance, please mount the volume to pg data directory by adding the option like `-v $PWD/pgdata:/var/lib/postgresql/data`

You can configure PostgreSQL by [the reference of the parent image](https://hub.docker.com/_/postgres/).

## Install from source

1. Please read [Development](./development.md) to setup a development environment.

2. Install pgvecto.rs with the following command:

```sh
cargo pgrx install --sudo --release
```

3. Configure your PostgreSQL by modifying the `shared_preload_libraries` to include `vectors.so`.

```sh
psql -U postgres -c 'ALTER SYSTEM SET shared_preload_libraries = "vectors.so"'
# You need restart the PostgreSQL cluster to take effects.
sudo systemctl restart postgresql.service   # for pgvecto.rs running with systemd
service postgresql restart  # for pgvecto.rs running in envd
```

4. Connect to the database and enable the extension.

```sql
DROP EXTENSION IF EXISTS vectors;
CREATE EXTENSION vectors;
```

## Install from release

1. Download the deb package in the release page, and type `sudo apt install vectors-pg15-*.deb` to install the deb package.

2. Configure your PostgreSQL by modifying the `shared_preload_libraries` to include `vectors.so`.

```sh
psql -U postgres -c 'ALTER SYSTEM SET shared_preload_libraries = "vectors.so"'
# You need restart the PostgreSQL cluster to take effects.
sudo systemctl restart postgresql.service   # for pgvecto.rs running with systemd
```

3. Connect to the database and enable the extension.

```sql
DROP EXTENSION IF EXISTS vectors;
CREATE EXTENSION vectors;
```
