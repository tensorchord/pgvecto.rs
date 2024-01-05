# Configuration

## Logging

By default, you cannot capture all pgvecto.rs logs. pgvecto.rs starts a background worker process for indexing, and it prints logs to standard error. To capture them, you need to set `logging_collector` to `on`. You can get more information from [PostgreSQL document about logging collector](https://www.postgresql.org/docs/current/runtime-config-logging.html#GUC-LOGGING-COLLECTOR).

You can set `logging_collector` to `on` with the following command:

```sh
psql -U postgres -c 'ALTER SYSTEM SET logging_collector = on;'
# You need restart the PostgreSQL cluster to take effects.
sudo systemctl restart postgresql.service   # for pgvecto.rs running with systemd
docker restart pgvecto-rs-demo  # for pgvecto.rs running in docker
```
