# Operations

[Documentation index](README.md) · [Configuration reference](configuration.md)

Commands below assume the repository root. They are examples to run when you intend to start a local service; writing these docs does not start a process or change a deployment.

## Local hot-only service

Build with Go and a working CGO toolchain. The DuckDB dependency requires the Arrow tag even though the application has no no-Arrow stub.

```sh
CGO_ENABLED=1 go build -tags=duckdb_arrow -o beehive-data-api .
export INFLUXDB_URL='http://localhost:8086'
export INFLUXDB_BUCKET='your-bucket'
# Supply INFLUXDB_TOKEN through your environment.
./beehive-data-api -addr=:10000
```

The Influx organization is `waggle`. Use `COLD_FRONTIER=static` (the default) and leave `COLD_WAREHOUSE`, `COLD_BORDER` and the three `HOT_TO_COLD_LOADER_*` connection settings unset for a hot-only service. A missing coordinator URL in dynamic mode is a startup error, not an Influx-only fallback. RabbitMQ is needed only for the live SSE route, not stored queries. Set `RABBITMQ_URL` to your broker URL to enable successful stream requests. No separate health-check endpoint is implemented; `/` is a redirect, and a successful metrics response alone does not prove storage availability.

Use a known small absolute-bound query from the [API guide](api.md) to check storage access and decode the actual response format. Do not use an entire month as a startup health check.

## Docker

```sh
docker build -t beehive-data-api:local .
docker run --rm --name beehive-data-api-local \
  -p 10000:10000 \
  -e INFLUXDB_URL -e INFLUXDB_TOKEN -e INFLUXDB_BUCKET \
  beehive-data-api:local
```

Export these environment values before running the container. A container's `localhost` is the container itself: choose an Influx URL reachable from its network. The image builds with CGO and `-tags=duckdb_arrow`, copies nested Go packages, and runs `/beehive-data-api` on Debian Bookworm with CA certificates. The matching builder/runtime base avoids glibc incompatibility.

## Static cold frontier

Point the API at an already-published Iceberg v3 warehouse. These values are illustrative; substitute the warehouse and border validated for your stores.

```sh
export COLD_WAREHOUSE='https://storage.example.org/sage/example-isolated-prefix/warehouse'
export COLD_FRONTIER='static'
export COLD_BORDER='2025-05-01T00:00:00Z'
export PELICAN_KEY_PATH='/path/to/trusted-signing-key.pem'
export COLD_ASYNC_THREADS='8'
export COLD_READ_AHEAD_DEPTH='-1'
export ARROW_COMPRESSION='zstd'
./beehive-data-api
```

Leave all three `HOT_TO_COLD_LOADER_*` connection settings unset for this mode.

The key must be an EC private key accepted by the configured Pelican trust setup, not an arbitrary newly generated key. See [configuration](configuration.md#cold-routing-and-credentials). In Docker, mount the key read-only at the path named by `PELICAN_KEY_PATH` and forward the cold settings. Keep private keys out of the image.

At startup the cold backend checks the key/scope, opens DuckDB, installs/loads `httpfs` and `iceberg`, and verifies the linked engine is 2.0.x. Extension installation needs download access unless cached. Actual hint, manifest and object reads happen during queries.

Coordinate the border with hot retention and published cold coverage. The API does not detect missing days or verify that hot data exists beyond the boundary. The warehouse must satisfy the [reader contract](architecture.md#cold-storage-contract); old split-value warehouses are not upgraded by changing the URL.

The API has read-only warehouse behavior. Publishing or testing writers belongs in a separate, fresh isolated prefix. Never use `/node-data` for experiments. Do not repoint a running service merely to follow a documentation example.

## Dynamic cold frontier

For continuous archival, configure the same bucket and warehouse as `beehive-hot-to-cold-loader`. The API reads data directly from Influx and Pelican; the loader supplies the routing view and read-lease coordination.

```sh
export COLD_FRONTIER='dynamic'
unset COLD_BORDER
export COLD_WAREHOUSE='https://storage.example.org/sage/example-managed-prefix/warehouse'
export HOT_TO_COLD_LOADER_URL='http://beehive-hot-to-cold-loader:10001'
export HOT_TO_COLD_LOADER_READER_OWNER='api-pod-uid'
export INFLUXDB_BUCKET='waggle'
export PELICAN_KEY_PATH='/path/to/trusted-signing-key.pem'
# Supply HOT_TO_COLD_LOADER_READER_TOKEN and INFLUXDB_TOKEN through secrets.
./beehive-data-api
```

Supply a unique pod/instance owner prefix; the API appends a process UUID and logs the resulting owner. Use the coordinator's reader-role secret, distinct from writer/admin secrets. The API still needs a trusted Pelican signing key. In Docker, forward these variables, mount the key read-only and use coordinator/ Influx addresses reachable from the container network. Configuration is checked at startup; actual coordinator access is checked on each query.

Each query pins one border and snapshot. Background renewal keeps that view valid during setup and streaming. Coordinator acquire errors fail the request; failed renewal cancels it at the last confirmed expiry. There is no fallback to static routing. Successful resource closure releases the grant; crashed or uncertain reads stop blocking deletion after lease expiry and the one-minute grace. Restarting the coordinator preserves confirmed deadlines.

Deploy the renewal-aware API and coordinator together, keep clocks synchronized, and drain/stop static or older API instances before enabling archival deletion. For options, mode comparisons and exact lease timing, see [Dynamic vs Static Cold Frontier](configuration.md#dynamic-vs-static-cold-frontier).

## Memory, concurrency and timeouts

Cold SQL uses two workers, eight async I/O threads by default, automatic memory-budgeted read-ahead and a 3 GB DuckDB limit. The backend serializes cold queries across both formats and holds its lock through result cleanup.

The engine limit is not an upper bound on container memory. Sort spill, native buffers, encoding, the Go runtime and charged file cache remain relevant. Provision and measure a container limit separately; the repository's memory setting is not a guarantee that a month-scale NDJSON query will fit.

`INFLUXDB_TIMEOUT` configures the hot client's duration. Hint discovery uses a 30-second HTTP client; request contexts control cancellation. Cold read tokens expire after 20 minutes. These are different mechanisms, not one global query timeout. The root HTTP server does not configure explicit server-wide timeout fields; any reverse-proxy deadlines must be considered separately.

In dynamic mode, `--read-lease-duration` on the coordinator defaults to `30m`. The API renews at most one minute apart under normal defaults; short leases use one-third of remaining time. This does not impose a fixed query runtime or refresh the Pelican token. Renewal stops on cancellation/closure. A lost lease aborts the query even if data is still arriving; healthy renewals do not extend the separate Influx timeout or proxy deadlines. Renewal traffic runs in the background, but its latency impact under production concurrency still needs measurement.

## Profiling cold Arrow queries

Set the local profile parent before starting the process:

```sh
export COLD_PROFILE_DIR='/tmp/beehive-cold-profiles'
./beehive-data-api
```

Profiling is off when the setting is empty. Each startup creates a private `session-*` directory. Each cold Arrow request gets a `.duckdb.json` operator profile and `.timing.json` sidecar. A container needs a writable mounted profile directory if files should survive recreation. Copy profiles out before replacing a container.

Credential setup precedes native profiling. Profiles can contain query SQL, warehouse URLs and local paths, so treat the files as operational data. Profiling does not change the response schema and does not profile the NDJSON path. A per-query profile-creation failure is logged; that request can proceed without a profile.

Sidecars record engine version, rows/batches, phase timings, whether the native profile is present, and status: `complete`, `failed`, `cancelled`, or `incomplete`. Use complete profiles for performance analysis. Setup, first batch and EOF timestamps overlap; operator work overlaps across workers and can include waits. Do not add these numbers together or interpret engine byte counters as direct Pelican network traffic. On DuckDB 2.0, execution can block inside the first fetch after a native result has already been returned.

## Metrics and logs

`GET /metrics` exposes Prometheus collectors, including Go/process metrics. Existing `dataapi_response_latency_seconds` and `dataapi_stream_connections_total` names, types and observation points remain compatible. Additional metrics distinguish Arrow/NDJSON latency and full handler duration, backend setup/cleanup, query outcomes/in-progress/rows/bytes, SSE sessions/delivery errors and reader renewal. See [API metrics and Grafana](metrics.md) for exact definitions, reset behavior, [the dashboard](../observability/grafana/beehive-data-api.json), provisioning and scrape/alert examples.

Use `dataapi_time_to_first_body_write_seconds{format}` for the first body bytes accepted by the response writer, and `dataapi_query_duration_seconds{format,outcome}` for full server lifetime through cleanup. Neither measures client dataframe completion or first decoded Arrow batch. The labeled legacy observation `dataapi_response_latency_by_format_seconds` retains its original timing points for comparison with earlier monitoring.

Query logs include the request body, backend errors, served record count and elapsed service time. These are not the client's dataframe-completion timer. The API does not implement inbound bearer-token validation itself; gateway access policy is separate from its outbound Pelican credentials.

Dynamic query logs also identify the reader owner, unresolved acquires and renewal failures. Renewal counters and latency are observed here so network failures are visible even when the coordinator receives nothing. Canceled requests are separate from errors. The hot-to-cold loader's durable job metrics and Grafana dashboard are separate; see its [metrics guide](https://github.com/waggle-sensor/beehive-hot-to-cold-loader/blob/main/docs/metrics.md). JSON cold-query profiles remain independent of read leases and the archival service's own profiles.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Missing `arrowmapping` or Arrow symbols while building | Use `CGO_ENABLED=1` and `-tags=duckdb_arrow` for build, tests and vet |
| `GLIBC_* not found` | Use the matched Bookworm builder/runtime in the Dockerfile |
| Cold startup fails | Valid frontier mode; static border or complete dynamic connection settings; readable EC PEM, supported engine, extension-download access |
| Dynamic configuration rejected | Set `COLD_FRONTIER=dynamic`, warehouse, bucket, URL, reader token and owner; unset `COLD_BORDER` |
| Static configuration rejected | Unset loader connection settings; set a valid border with a nonempty warehouse, or clear both for hot-only operation |
| Dynamic query fails before data | Coordinator reachability/reader token, matching bucket/warehouse, coverage start and metadata URL |
| Query canceled after renewal errors | Coordinator connectivity/journal health and synchronized clocks; retry the complete query with a fresh request |
| Old reader remains in coordinator state | Check its expiry and grace; expired rows may remain for diagnosis without blocking deletion |
| Hint or object read fails | Warehouse URL layout, published hint, rebased manifest references, scope/trusted key and network reachability |
| No first batch on a long query | Complete query profile; scan/sort can block before streaming begins |
| Container exits 137 or is OOM-killed | Container limit/other processes and cgroup counters; DuckDB's 3 GB is not a total-process limit |
| NDJSON finishes with too few rows | Check server errors and independent expected counts; HTTP 200 is insufficient |
| Head/tail or aggregation crosses border | Narrow to one tier or use an ordinary extract |
| Direct relative query fails | Send absolute RFC3339 bounds; the client resolves relative times before transport |
| Stream returns 502 | RabbitMQ URL, exchange `waggle.msg`, and broker connectivity |
