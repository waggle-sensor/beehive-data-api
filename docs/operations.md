# Operations

[Documentation index](README.md) · [Configuration reference](configuration.md)

Commands below assume the repository root. They are examples to run when you
intend to start a local service; writing these docs does not start a process
or change a deployment.

## Local hot-only service

Build with Go and a working CGO toolchain. The DuckDB dependency requires the
Arrow tag even though the application has no no-Arrow stub.

```sh
CGO_ENABLED=1 go build -tags=duckdb_arrow -o beehive-data-api .
export INFLUXDB_URL='http://localhost:8086'
export INFLUXDB_BUCKET='your-bucket'
# Supply INFLUXDB_TOKEN through your environment.
./beehive-data-api -addr=:10000
```

The Influx organization is `waggle`. Leave `COLD_WAREHOUSE` unset for a hot-only
service. RabbitMQ is needed only for the live SSE route, not stored queries.
Set `RABBITMQ_URL` to your broker URL to enable successful stream requests.
No separate health-check endpoint is implemented; `/` is a redirect, and a
successful metrics response alone does not prove storage availability.

Use a known small absolute-bound query from the [API guide](api.md) to check
storage access and decode the actual response format. Do not use an entire
month as a startup health check.

## Docker

```sh
docker build -t beehive-data-api:local .
docker run --rm --name beehive-data-api-local \
  -p 10000:10000 \
  -e INFLUXDB_URL -e INFLUXDB_TOKEN -e INFLUXDB_BUCKET \
  beehive-data-api:local
```

Export these environment values before running the container. A container's
`localhost` is the container itself: choose an Influx URL reachable from its
network. The image builds with CGO and `-tags=duckdb_arrow`, copies nested Go
packages, and runs `/beehive-data-api` on Debian Bookworm with CA certificates.
The matching builder/runtime base avoids glibc incompatibility.

## Enable the cold tier

Point the API at an already-published Iceberg v3 warehouse. These values are
illustrative; substitute the warehouse and watermark validated for your stores.

```sh
export COLD_WAREHOUSE='https://storage.example.org/sage/example-isolated-prefix/warehouse'
export COLD_WATERMARK='2025-05-01T00:00:00Z'
export PELICAN_KEY_PATH='/path/to/trusted-signing-key.pem'
export COLD_ASYNC_THREADS='8'
export COLD_READ_AHEAD_DEPTH='-1'
export ARROW_COMPRESSION='zstd'
./beehive-data-api
```

The key must be an EC private key accepted by the configured Pelican trust
setup, not an arbitrary newly generated key. See
[configuration](configuration.md#cold-routing-and-credentials). In Docker,
mount the key read-only at the path named by `PELICAN_KEY_PATH` and forward
the cold settings. Keep private keys out of the image.

At startup the cold backend checks the key/scope, opens DuckDB, installs/loads
`httpfs` and `iceberg`, and verifies the linked engine is 2.0.x. Extension
installation needs download access unless cached. Actual hint, manifest and
object reads happen during queries.

Coordinate the watermark with hot retention and published cold coverage.
The API does not detect missing days or verify that hot data exists beyond
the boundary. The warehouse must satisfy the
[reader contract](architecture.md#cold-storage-contract); old split-value
warehouses are not upgraded by changing the URL.

The API has read-only warehouse behavior. Publishing or testing writers belongs
in a separate, fresh isolated prefix. Never use `/node-data` for experiments.
Do not repoint a running service merely to follow a documentation example.

## Memory, concurrency and timeouts

Cold SQL uses two workers, eight async I/O threads by default, automatic
memory-budgeted read-ahead and a 3 GB DuckDB limit. The backend serializes cold
queries across both formats and holds its lock through result cleanup.

The engine limit is not an upper bound on container memory. Sort spill,
native buffers, encoding, the Go runtime and charged file cache remain
relevant. Provision and measure a container limit separately; the repository's
memory setting is not a guarantee that a month-scale NDJSON query will fit.

`INFLUXDB_TIMEOUT` configures the hot client's duration. Hint discovery uses a
30-second HTTP client; request contexts control cancellation. Cold read tokens
expire after 20 minutes. These are different mechanisms, not one global query
timeout. The root HTTP server does not configure explicit server-wide timeout
fields; any reverse-proxy deadlines must be considered separately.

## Profiling cold Arrow queries

Set the local profile parent before starting the process:

```sh
export COLD_PROFILE_DIR='/tmp/beehive-cold-profiles'
./beehive-data-api
```

Profiling is off when the setting is empty. Each startup creates a private
`session-*` directory. Each cold Arrow request gets a `.duckdb.json` operator
profile and `.timing.json` sidecar. A container needs a writable mounted
profile directory if files should survive recreation. Copy profiles out
before replacing a container.

Credential setup precedes native profiling. Profiles can contain query SQL,
warehouse URLs and local paths, so treat the files as operational data.
Profiling does not change the response schema and does not profile the NDJSON
path. A per-query profile-creation failure is logged; that request can proceed
without a profile.

Sidecars record engine version, rows/batches, phase timings, whether the native
profile is present, and status: `complete`, `failed`, `cancelled`, or
`incomplete`. Use complete profiles for performance analysis. Setup, first
batch and EOF timestamps overlap; operator work overlaps across workers and
can include waits. Do not add these numbers together or interpret engine byte
counters as direct Pelican network traffic. On DuckDB 2.0, execution can block
inside the first fetch after a native result has already been returned.

## Metrics and logs

`GET /metrics` exposes Prometheus collectors, including Go/process metrics.
The application adds:

| Metric | Actual observation |
| --- | --- |
| `dataapi_response_latency_seconds` | Histogram: for Arrow, observed before IPC writing; for NDJSON, at the first row about to be written |
| `dataapi_stream_connections_total` | Gauge of active stream handlers despite the `_total` suffix |

The Arrow histogram is not a measurement of the first decoded Arrow batch:
a blocking fetch can happen after the observation. The NDJSON path does not
observe this latency for an empty response. Neither metric is a full-response
latency measurement.

Query logs include the request body, backend errors, served record count and
elapsed service time. These are not the client's dataframe-completion timer.
The API does not implement inbound bearer-token validation itself; gateway
access policy is separate from its outbound Pelican credentials.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Missing `arrowmapping` or Arrow symbols while building | Use `CGO_ENABLED=1` and `-tags=duckdb_arrow` for build, tests and vet |
| `GLIBC_* not found` | Use the matched Bookworm builder/runtime in the Dockerfile |
| Cold startup fails | Required watermark/key, readable EC PEM, supported engine, extension-download access |
| Hint or object read fails | Warehouse URL layout, published hint, rebased manifest references, scope/trusted key and network reachability |
| No first batch on a long query | Complete query profile; scan/sort can block before streaming begins |
| Container exits 137 or is OOM-killed | Container limit/other processes and cgroup counters; DuckDB's 3 GB is not a total-process limit |
| NDJSON finishes with too few rows | Check server errors and independent expected counts; HTTP 200 is insufficient |
| Head/tail or aggregation crosses watermark | Narrow to one tier or use an ordinary extract |
| Direct relative query fails | Send absolute RFC3339 bounds; the client resolves relative times before transport |
| Stream returns 502 | RabbitMQ URL, exchange `waggle.msg`, and broker connectivity |
