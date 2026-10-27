# Configuration reference

[Documentation index](README.md) · [Operations](operations.md)

Flags normally override environment defaults. An explicitly set empty
environment variable is retained rather than replaced by the fallback.
Duration environment values are parsed while flags are registered, so an
invalid duration environment value can fail startup even if a valid flag
would otherwise override it.

## Startup options

| CLI flag | Environment variable | Default | Meaning / validation |
| --- | --- | --- | --- |
| `-addr` | None | `:10000` | HTTP listen address |
| `-arrow.compression` | `ARROW_COMPRESSION` | `zstd` | `none`, `zstd`, or `lz4`; trims whitespace, case-insensitive; empty also selects Zstd; invalid values fail startup |
| `-influxdb.url` | `INFLUXDB_URL` | `http://localhost:8086` | InfluxDB URL |
| `-influxdb.token` | `INFLUXDB_TOKEN` | empty | Influx API token |
| `-influxdb.bucket` | `INFLUXDB_BUCKET` | empty | Default hot bucket; requests can override it |
| `-influxdb.timeout` | `INFLUXDB_TIMEOUT` | `15m` | Influx HTTP client duration; invalid syntax fails parsing |
| `-cold.warehouse` | `COLD_WAREHOUSE` | empty | Warehouse base URL; nonempty enables tiered routing |
| `-cold.async-threads` | `COLD_ASYNC_THREADS` | `8` | Integer from 1 through 256; checked when cold is enabled |
| `-cold.read-ahead-depth` | `COLD_READ_AHEAD_DEPTH` | `-1` | `-1` automatic memory-budgeted lookahead, or `0` disabled; checked when cold is enabled |
| `-cold.profile-dir` | `COLD_PROFILE_DIR` | empty | Local profile parent directory; empty disables profiling |
| `-cold.watermark` | `COLD_WATERMARK` | empty | Required with a warehouse; RFC3339 boundary, including fractional seconds |
| `-pelican.key` | `PELICAN_KEY_PATH` | empty | Required with a warehouse; readable PEM EC signing key |
| `-rabbitmq.url` | `RABBITMQ_URL` | empty | RabbitMQ URL used by the live SSE endpoint |
| `-stream.heartbeat-duration` | `STREAM_HEARTBEAT_DURATION` | `15s` | SSE idle heartbeat duration; use a positive duration |

Go duration examples include `15s`, `2m`, and `15m`. The heartbeat is not
validated as positive at startup; a nonpositive value is invalid when the
stream handler creates its ticker.

The Influx organization is fixed to `waggle`; there is no organization flag.
There is no environment variable for `-addr`. The process can start without
usable hot credentials or RabbitMQ connectivity; those operations can fail
when requested. Enabling cold storage additionally opens DuckDB, loads
extensions, validates the warehouse scope/key and checks the engine version
at startup. It does not prove every remote data object is accessible.

## Cold routing and credentials

The watermark separates two half-open ranges: cold before the boundary and
hot at or after it. No automatic watermark discovery or retention validation
occurs. Operators must ensure the selected hot/cold stores have the intended
coverage; the router does not deduplicate overlapping archives or fill gaps.

The reader expects the table metadata beneath
`<COLD_WAREHOUSE>/sage/obs/metadata/`. The signing scope is derived from the
warehouse prefix, for the deployed URL form
`https://<host>/sage/<isolated-prefix>/warehouse`. See the
[storage contract](architecture.md#cold-storage-contract).

The configured PEM must correspond to a key trusted by Pelican. The current
signer uses fixed issuer `https://sagecontinuum.org`, key ID `3cfa`, ES256,
and a scoped read token. These are not configurable startup flags. Keep real
tokens and private keys out of example files and images.

## Fixed engine settings

These come from the cold runtime setup, not CLI options:

| Setting | Value |
| --- | --- |
| Supported embedded DuckDB version | `2.0.x`; currently pinned to `2.0.0` |
| CPU worker threads | `2` |
| DuckDB memory limit | `3GB` |
| Session timezone | `UTC` |
| Arrow export format | `1.0` |
| Extensions | `httpfs`, `iceberg` |
| Native Arrow engine | Reused per cold backend |
| Cold query concurrency | Serialized by the backend lock across both formats |

The 3 GB engine limit is not a process/container limit: native buffers,
encoding, runtime memory and filesystem cache can add to it. Set a container
budget separately. Turning lookahead off does not turn asynchronous I/O off.
Fixed positive read-ahead depths are not exposed.

There is no operator option for uniform-metadata discovery, split-value Arrow
schemas, windowed cold execution, engine fallback, or a per-request codec.

Source of truth: [startup options](../main.go) and
[cold runtime settings](../internal/backend/cold/settings.go).
