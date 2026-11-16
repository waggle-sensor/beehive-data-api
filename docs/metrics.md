# API metrics and Grafana

[Documentation index](README.md) · [Operations](operations.md)

`GET /metrics` exposes the existing Prometheus registry. Query metrics distinguish `format="arrow"` and `format="ndjson"` without changing either public response schema. These labels describe the selected query format; rejected requests can still return a plain-text error body. Metrics cover the query and SSE handlers, not every HTTP path served by a proxy.

## Existing metrics remain compatible

| Metric | Preserved behavior |
| --- | --- |
| `dataapi_response_latency_seconds` | Original unlabeled histogram. It uses Prometheus' default buckets and the original timing points: Arrow before IPC encoding; NDJSON before the first row write. Empty NDJSON has no observation. Arrow and NDJSON share this one series. |
| `dataapi_stream_connections_total` | Original gauge of active GET/Flusher-supported SSE handlers, including broker setup, despite the `_total` suffix. |
| `hot_to_cold_read_renewals_total{outcome}` | API-side renewal counter using the documented name; outcomes `success`, `error`, `canceled`. |
| `hot_to_cold_read_renewal_duration_seconds{outcome}` | API-side renewal attempt duration, including transport/validation work. |
| `go_*`, `process_*` | Standard Go/runtime and supported process collectors. |

Existing dashboards using the combined latency histogram and SSE gauge can continue using them. The labeled latency family below is a separate name; no label was added to the original family.

## Query observations

Names below have prefix `dataapi_`:

| Suffix | Type / labels | Observation |
| --- | --- | --- |
| `response_latency_by_format_seconds` | Histogram, `format` | Same points as the legacy latency histogram, now separated by format. |
| `time_to_first_body_write_seconds` | Histogram, `format` | Time from handler entry until the response writer accepts its first nonempty HTTP 200 body write. |
| `query_duration_seconds` | Histogram, `format,outcome` | Entire handler lifetime through body/result cleanup and any read-grant release. |
| `backend_query_duration_seconds` | Histogram, `format,outcome` | Time inside the backend's `Query` call, including routing/setup and any lock wait. |
| `result_close_duration_seconds` | Histogram, `format,outcome` | Result closure and resource/read-grant release. |
| `queries_total` | Counter, `format,outcome,status` | Finished handlers, including rejected requests and aborted streams. |
| `queries_in_progress` | Gauge, `format` | Current handlers, including setup, waiting inside a backend, streaming and cleanup. |
| `query_oldest_in_progress_timestamp_seconds` | Gauge, `format` | Oldest active handler start, zero when idle; use it to detect long-running requests before they finish. |
| `query_errors_total` | Counter, `format,stage` | Non-cancellation error observations by bounded stage. One request can have multiple errors. |
| `query_response_bytes_total` | Counter, `format` | Body bytes accepted by the response writer, published in 64 KiB increments and at handler exit. |
| `query_rows_total` | Counter, `format,outcome` | Encoder-reported processed rows, published when the handler ends. |
| `query_response_size_bytes` | Histogram, `format,outcome` | Accepted body bytes per finished handler, including error bodies. |
| `query_result_rows` | Histogram, `format,outcome` | Encoder-reported rows per finished handler, including zero for pre-result failures. |
| `configuration_info` | Constant gauge 1, `cold_frontier,arrow_compression` | Startup configuration: hot-only, static or dynamic frontier; selected Arrow IPC codec. |
| `build_info` | Constant gauge 1, `version,revision,dirty` | Available binary build metadata; unknown is allowed. |

Query outcomes are `success`, `invalid_request`, `backend_error`, `stream_error`, `close_error`, `canceled`, `panic`. `status` is the observed `200`, `400`, `500`, `none` (panic before headers) or `other`, not an unbounded status string. Stages are `request`, `parse`, `backend`, `encode`, `iterate`, `write`, `close`, `panic`. Backend/close timing outcomes are `success`, `error`, `canceled`. Cancellation is identified from request context or typed cancellation/deadline errors associated with failed operations; a fully encoded response is not marked canceled merely because the caller disconnects during subsequent successful cleanup. Write failures can also represent disconnected clients. Cancellations have their own query outcome rather than incrementing the stage-error counter.

Latency measurements answer different questions:

- Legacy latency is the old observation, suitable for continuity with earlier dashboards. Its Arrow and NDJSON points are different, and both formats share the unlabeled series. The format-specific copy uses the same timing points and separate buckets, from 5 ms through 3600 s.
- First accepted body write includes blocking before that write, but Arrow can emit schema bytes before rows. It is not first decoded batch, socket delivery, proxy receipt or client dataframe time. Empty NDJSON has no body/sample; empty Arrow still writes an IPC stream.
- Full handler duration includes cleanup after encoding. A successful backend `Query` return does not imply scanning is complete: native fetch/sort work can happen later. An in-progress request is not necessarily actively scanning.

HTTP status alone cannot establish success. A late streaming failure keeps the already-written 200 status and records `stream_error`/`canceled`; framing is aborted so clients cannot accept a complete extract. A cleanup failure records `close_error` without claiming an already-written response was incomplete. Panic observations preserve the original panic/abort behavior.

Rows/bytes are server observations, not acknowledgements from clients. For aborted Arrow responses, the encoder's returned row count can include buffered rows that were never emitted; use `outcome="success"` for completed-extract throughput. Even completed extracts do not prove the caller decoded them. Bytes exclude HTTP headers/framing and later proxy transformations. Arrow IPC compression is included in body size; these are not Parquet scan bytes.

Metrics avoid raw queries, filters, measurements, VSNs, owners, request IDs, URLs and error-text labels. Configuration labels come from fixed startup enums; build labels are constant per binary. `cold_frontier="hot"` means cold storage is disabled; it is not an additional accepted `COLD_FRONTIER` setting. Counters/histograms reset on process restart; use `rate`/`increase` rather than subtracting raw counters. Interrupted processes may leave no final duration/row observation. Byte counters are chunked and can miss the unpublished tail if the process dies. The native Arrow stream remains native; metrics add no Go row reconstruction or per-measurement metric increment. These operational histograms do not control query size or tier; compare matched workloads before drawing Arrow-versus-NDJSON conclusions.

## SSE observations

The format selector does not apply to live SSE:

| Metric | Meaning |
| --- | --- |
| `dataapi_stream_connections_started_total` | GET/Flusher-supported setup attempts, including ones that fail broker setup. |
| `dataapi_stream_sessions_total{outcome}` | Ended handler attempts. |
| `dataapi_stream_session_duration_seconds{outcome}` | Handler lifetime through broker cleanup; samples appear when a session ends. |
| `dataapi_stream_messages_total{outcome}` | Broker deliveries `written`, `filtered` or `invalid`. Written means event write/flush completed, not client acknowledgement. |
| `dataapi_stream_errors_total{stage}` | Request/filter/broker/decode/encode/write/flush/closed-channel/panic errors by fixed stage. |
| `dataapi_stream_response_bytes_total` | Accepted event and heartbeat body bytes, including bytes before a later flush failure. |
| `dataapi_stream_heartbeats_total` | Heartbeats written and flushed without an error. |

Session outcomes are `canceled`, `invalid_request`, `broker_error`, `write_error`, `unsupported` (no Flusher) and `panic`. Client cancellation is normal session termination. Error stages are `request`, `filter`, `dial`, `channel`, `queue`, `bind`, `consume`, `decode`, `encode`, `write`, `broker_close`, `panic`. Malformed broker messages count as invalid deliveries while the session can continue. A closed subscription channel ends the session as a broker error; write/flush errors end output. Broker acknowledgement behavior is unchanged.

## Process resources and renewal

Process CPU/RSS includes embedded native DuckDB, not just Go. Go heap metrics exclude native allocations; subtracting heap from RSS is not a precise DuckDB memory measurement. RSS is also not a container/cgroup memory limit or its charged file cache. Standard process collectors may be absent on unsupported platforms such as macOS; the Linux deployment supplies these metrics.

Dynamic read renewal metrics run on the API side so transport failures are visible even when the coordinator receives nothing. The coordinator itself publishes separate job/reader metrics. Renewal observation never authorizes a renewal, changes a pinned view or extends a confirmed deadline. A static/hot-only API normally has zero renewal traffic.

## Grafana and Prometheus

Import [Beehive Data API dashboard JSON](../observability/grafana/beehive-data-api.json) or use the [file-provisioning provider](../observability/grafana/provisioning/dashboards.yaml). It uses schema version 38 and standard panels, targeting Grafana 10 or later. Verify import/rendering against the actual deployed Grafana version before rollout.

For provisioning, mount the provider into Grafana's dashboards provisioning directory and mount JSON under `/var/lib/grafana/dashboards/beehive-data-api/`. Stable UID `beehive-data-api` updates the same dashboard; do not overwrite an unrelated existing dashboard with that UID. No datasource or credentials are embedded or overwritten. Select an existing Prometheus datasource, API job, instance and response format. Selectors use the legacy latency family so older API deployments can still be selected; their newer panels show no data rather than invented zeros. All queries scope to the selected targets and preserve instance separation. SSE, renewal, runtime/configuration and original combined-latency panels intentionally ignore the format filter.

The dashboard defaults to 24 hours and a 30-second refresh. Panels cover QPS, outcomes/status, in-progress queries, format-specific latencies, throughput/size, cleanup/errors, SSE, renewal and process resources. Empty denominators and missing samples display no data/unknown rather than healthy zeros. Quantiles are bucket estimates; inspect sample counts for sparse workloads. Time-range increases only cover events observed after scraping begins.

Reuse an existing API scrape or merge [the scrape example](../observability/prometheus/scrape.yaml). The API's own `/metrics` requires no authentication; deployment gateway policies are separate. No Pushgateway or new job manager is introduced.

[Alert examples](../observability/prometheus/alerts.yaml) cover reachability, server/stream/cleanup failures, SSE broker failures and renewal errors. Tune thresholds/job selectors to traffic and SLOs. Failure-ratio rules exclude invalid requests/cancellations and require a minimum sample count. Keep operational resource-close failures visible separately from client-visible extract failures.

```sh
promtool check config observability/prometheus/scrape.yaml
promtool check rules observability/prometheus/alerts.yaml
```

Exposition/metric-reference tests validate the dashboard against actual exported families. PromQL rule-engine checks and live Grafana rendering still require their tools. Test normal/empty responses, failed setup, partial output, canceled queries, multiple instances and restarts before deploying the dashboard.
