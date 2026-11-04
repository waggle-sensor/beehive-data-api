# Configuration reference

[Documentation index](README.md) · [Operations](operations.md)

Flags normally override environment defaults. An explicitly set empty environment variable is retained rather than replaced by the fallback. Duration environment values are parsed while flags are registered, so an invalid duration environment value can fail startup even if a valid flag would otherwise override it.

## Startup options

| CLI flag | Environment variable | Default | Meaning / validation |
| --- | --- | --- | --- |
| `-addr` | None | `:10000` | HTTP listen address |
| `-arrow.compression` | `ARROW_COMPRESSION` | `zstd` | `none`, `zstd`, or `lz4`; trims whitespace, case-insensitive; empty also selects Zstd; invalid values fail startup |
| `-influxdb.url` | `INFLUXDB_URL` | `http://localhost:8086` | InfluxDB URL |
| `-influxdb.token` | `INFLUXDB_TOKEN` | empty | Influx API token |
| `-influxdb.bucket` | `INFLUXDB_BUCKET` | empty | Default hot bucket; static requests can override it; dynamic requests must match it |
| `-influxdb.timeout` | `INFLUXDB_TIMEOUT` | `15m` | Influx HTTP client duration; invalid syntax fails parsing |
| `-cold.warehouse` | `COLD_WAREHOUSE` | empty | Warehouse base URL; nonempty enables tiered routing |
| `-cold.async-threads` | `COLD_ASYNC_THREADS` | `8` | Integer from 1 through 256; checked when cold is enabled |
| `-cold.read-ahead-depth` | `COLD_READ_AHEAD_DEPTH` | `-1` | `-1` automatic memory-budgeted lookahead, or `0` disabled; checked when cold is enabled |
| `-cold.profile-dir` | `COLD_PROFILE_DIR` | empty | Local profile parent directory; empty disables profiling |
| `-cold.frontier` | `COLD_FRONTIER` | `static` | `dynamic` or `static`; trims whitespace and is case-insensitive; invalid/empty values fail startup |
| `-cold.border` | `COLD_BORDER` | empty | Required with a warehouse in static mode; RFC3339 boundary, including fractional seconds; must be unset in dynamic mode |
| `-hot-to-cold-loader.url` | `HOT_TO_COLD_LOADER_URL` | empty | Required in dynamic mode; unset in static mode |
| `-hot-to-cold-loader.reader-token` | `HOT_TO_COLD_LOADER_READER_TOKEN` | empty | Reader-role coordination credential; required in dynamic mode, unset in static mode |
| `-hot-to-cold-loader.reader-owner` | `HOT_TO_COLD_LOADER_READER_OWNER` | empty | Dynamic reader owner prefix; use pod UID; a process UUID is appended automatically |
| `-pelican.key` | `PELICAN_KEY_PATH` | empty | Required with a warehouse; readable PEM EC signing key |
| `-rabbitmq.url` | `RABBITMQ_URL` | empty | RabbitMQ URL used by the live SSE endpoint |
| `-stream.heartbeat-duration` | `STREAM_HEARTBEAT_DURATION` | `15s` | SSE idle heartbeat duration; use a positive duration |

Go duration examples include `15s`, `2m`, and `15m`. The heartbeat is not validated as positive at startup; a nonpositive value is invalid when the stream handler creates its ticker.

The Influx organization is fixed to `waggle`; there is no organization flag. There is no environment variable for `-addr`. The process can start without usable hot credentials or RabbitMQ connectivity; those operations can fail when requested. Enabling cold storage additionally opens DuckDB, loads extensions, validates the warehouse scope/key and checks the engine version at startup. It does not prove every remote data object is accessible.

## Cold routing and credentials

The border separates two half-open ranges: cold before the boundary and hot at or after it. In static mode no automatic border discovery or retention validation occurs. Operators must ensure the selected hot/cold stores have the intended coverage; the router does not deduplicate overlapping archives or fill gaps.

The reader expects the table metadata beneath `<COLD_WAREHOUSE>/sage/obs/metadata/`. The signing scope is derived from the warehouse prefix, for the deployed URL form `https://<host>/sage/<isolated-prefix>/warehouse`. See the [storage contract](architecture.md#cold-storage-contract).

The configured PEM must correspond to a key trusted by Pelican. The current signer uses fixed issuer `https://sagecontinuum.org`, key ID `3cfa`, ES256, and a scoped read token. These are not configurable startup flags. Keep real tokens and private keys out of example files and images.

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

The 3 GB engine limit is not a process/container limit: native buffers, encoding, runtime memory and filesystem cache can add to it. Set a container budget separately. Turning lookahead off does not turn asynchronous I/O off. Fixed positive read-ahead depths are not exposed.

There is no operator option for uniform-metadata discovery, split-value Arrow schemas, windowed cold execution, engine fallback, or a per-request codec.

Source of truth: [startup options](../main.go), [frontier validation](../internal/backend/tiered/backend.go), and [cold runtime settings](../internal/backend/cold/settings.go).

## Dynamic vs Static Cold Frontier

`COLD_FRONTIER` explicitly selects how the API determines the hot/cold boundary.

| Behavior | `static` (default) | `dynamic` |
| --- | --- | --- |
| Boundary source | Operator-configured `COLD_BORDER` | Verified frontier returned by the hot-to-cold loader per query |
| Cold snapshot | Resolve the warehouse's metadata hint per query | Pin the exact snapshot returned with the read grant |
| Read coordination | No reader lease | Renewable reader lease held through query setup/streaming |
| Loader settings | Must be unset | URL, reader token and reader owner required |
| Appropriate use | Fixed demo/validation warehouses with operator-managed coverage | Continuous archival and coordinated Influx deletion |

Both modes read cold data before the boundary and hot data at or after it. They preserve the same public query and response schema. With no warehouse configured, the default static mode remains hot-only; leave `COLD_BORDER` unset too.

Static example (plus normal Influx configuration):

```sh
export COLD_FRONTIER=static
export COLD_WAREHOUSE='https://storage.example.org/sage/example-isolated-prefix/warehouse'
export COLD_BORDER='2025-05-01T00:00:00Z'
export PELICAN_KEY_PATH='/path/to/trusted-signing-key.pem'
```

Dynamic example (reader token supplied through deployment secrets):

```sh
export COLD_FRONTIER=dynamic
unset COLD_BORDER
export COLD_WAREHOUSE='https://storage.example.org/sage/example-managed-prefix/warehouse'
export HOT_TO_COLD_LOADER_URL='http://beehive-hot-to-cold-loader:10001'
export HOT_TO_COLD_LOADER_READER_OWNER='api-pod-uid'
export INFLUXDB_BUCKET='waggle'
export PELICAN_KEY_PATH='/path/to/trusted-signing-key.pem'
```

Set `HOT_TO_COLD_LOADER_READER_TOKEN` to the coordinator's reader-role secret. The Influx loader uses `HOT_TO_COLD_LOADER_URL` with `HOT_TO_COLD_LOADER_LOADER_TOKEN` and `HOT_TO_COLD_LOADER_LOADER_OWNER` for its separate writer role. In dynamic mode leave `COLD_BORDER` unset. In static mode leave all three `HOT_TO_COLD_LOADER_*` connection settings unset. Conflicting settings fail startup instead of being silently ignored. Never enable archival deletion while uncoordinated static API readers can still access the affected Influx bucket.

In dynamic mode, the API acquires an authenticated read grant per request. The grant pins a contiguous cold frontier and exact Iceberg metadata URL. Both routing and cold execution use that same view. The result retains the grant until its resources close, allowing the archiver to drain old hot readers before deletion. Native Arrow streaming is preserved.

Dynamic mode rejects requests before declared coverage and bucket overrides outside the configured bucket. Coordinator errors fail the query; there is no fallback to a static border.

### Renewable read leases

Grants carry `expires_at`, set by the coordinator's `--read-lease-duration` (default 30 minutes). This is a renewable lease, not a hard query limit. The API renews in the background every minute or one-third of the remaining lease, whichever is shorter, without changing the pinned view. It keeps renewing through query setup and streaming until resources close or the request is canceled. Healthy queries can outlive the initial deadline; existing caller/Influx timeouts still apply. If renewal fails, retries continue only until the last confirmed expiry, when the query is canceled and interrupted streams are aborted. Expired or released grants cannot be revived. Retry lost-lease queries with a new request. Unreleased grants stop blocking deletion one minute after expiry, including after API crashes or uncertain closure. Restarts preserve leases. Keep host clocks synchronized and deploy the renewal-aware API and coordinator together. See the [archiver operator guide](https://github.com/waggle-sensor/beehive-hot-to-cold-loader/blob/main/README.md).


The lease duration is configured on `beehive-hot-to-cold-loader`, not the API. An initial grant expires at acquisition time plus that duration. Each successful renewal refreshes expiry to renewal time plus the duration, without shortening an already later deadline. No further lease duration is added after the last confirmed deadline. With the default duration, a renewal confirmed at 12:10 gives expiry at 12:40; if renewals stop, deletion can proceed after grace at 12:41.

Renewal runs outside row/batch iteration, including while a request waits for cold execution. Each renewal request has a 30-second timeout bounded by the confirmed expiry. Temporary failures retry after at most one second (sooner near expiry). An independent watchdog cancels at the confirmed expiry even if a renewal request hangs. A still-active but stuck request can keep renewing; this is crash/connection-loss recovery, not progress-based stall detection.
