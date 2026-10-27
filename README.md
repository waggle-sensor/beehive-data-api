# Beehive Data API

Beehive Data API serves measurements from InfluxDB hot storage and, when
configured, an Iceberg v3 cold warehouse on Pelican. Queries return NDJSON or
an Arrow IPC stream with the same four fields across storage tiers. A separate
SSE endpoint streams live measurements from RabbitMQ.

These docs describe this source tree. Deployed services and released clients
may have different capabilities.

## Documentation

Start with the [documentation index](docs/README.md).

| Guide | Use it to |
| --- | --- |
| [API](docs/api.md) | Submit queries, decode responses, and consume live events |
| [Configuration](docs/configuration.md) | Look up flags, environment variables, and defaults |
| [Architecture](docs/architecture.md) | Understand packages, execution paths, and storage contracts |
| [Operations](docs/operations.md) | Run the service, configure cold storage, and diagnose queries |
| [Development](docs/development.md) | Build, test, and use native integration fixtures and probes |

## Build and run

From this directory, with Go, a C/C++ toolchain, and module-download access:

```sh
CGO_ENABLED=1 go build -tags=duckdb_arrow -o beehive-data-api .
export INFLUXDB_URL='http://localhost:8086'
export INFLUXDB_BUCKET='your-bucket'
# Set INFLUXDB_TOKEN in your environment before starting.
./beehive-data-api
```

The default address is `:10000`. Cold storage is disabled until
`COLD_WAREHOUSE` is set. See [operations](docs/operations.md) for complete local
and Docker examples and [configuration](docs/configuration.md) for required
cold settings.

`CGO_ENABLED=1` and `-tags=duckdb_arrow` are required for supported service
builds: DuckDB's dependency still gates `arrowmapping` behind that tag.
Docker and CI use it. The module name is `beehive-data-api`.

```sh
CGO_ENABLED=1 go test -tags=duckdb_arrow ./...
CGO_ENABLED=1 go vet -tags=duckdb_arrow ./...
```
