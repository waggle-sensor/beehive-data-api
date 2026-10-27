# API guide

[Documentation index](README.md)

Examples target the updated local service at `http://localhost:10000`.
Use data and time ranges available in your deployment. An HTTP client that
requests Arrow must inspect the actual response media type before decoding;
older deployments can still return NDJSON.
>NOTE: more details [here](https://github.com/waggle-sensor/waggle-beehive-v2/blob/main/docs/querying-measurements.md).

## Routes

| Route | Purpose |
| --- | --- |
| `POST /api/v1/query` | Query stored measurements |
| `GET /api/v0/stream` | Receive live measurement events over SSE |
| `GET /metrics` | Prometheus metrics |
| `/` | Redirect to the public data-access tutorial |

The query examples use POST. The current query handler does not explicitly
reject other methods; callers should use the documented POST interface.

## Query request

Send a JSON object. `start` is required; unknown fields are rejected.

```json
{
  "start": "2025-03-01T00:00:00Z",
  "end": "2025-03-01T00:01:00Z",
  "filter": {"name": "wxt.env.temp", "vsn": "W0A0"}
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `start` | string | Inclusive start instant; use an absolute RFC3339 timestamp |
| `end` | string | Exclusive end instant; may be omitted |
| `filter` | object of strings | Measurement name and metadata predicates combined with AND; omitted means no predicates |
| `head` | integer | First N rows per series; use either head or tail |
| `tail` | integer | Last N rows per series; use either head or tail |
| `bucket` | string | Override the hot Influx bucket; has no effect on the cold warehouse |
| `experimental_func` | string | Hot-side aggregation; see support matrix |
| `experimental_window` | string | Hot-side aggregation interval, requiring a function |

RFC3339 fractional seconds can retain nine digits, for example
`2025-03-01T00:00:00.026403194Z`. Cold queries preserve that nanosecond bound.
With tiered routing enabled, direct HTTP requests must use absolute bounds,
even for ranges that ultimately route to hot storage. Omitted `end` reaches
the hot tier; a cold execution with an omitted end resolves its bound to the
request's current time. The Python client resolves relative times before
sending a request.

Limits are per series, not a global response-row limit. Cold series are
partitioned by measurement name and assembled metadata; hot limits follow
Flux tables. Do not supply both `head` and `tail`. Cold limits must be >= 0;
zero selects no rows. Hot-only validation ultimately also depends on Flux.

### Filters

Use `name` for the measurement and bare metadata keys such as `vsn`, `node`,
`plugin`, or `sensor`. Do not use dataframe column names such as `meta.vsn`.
Filter keys must match `^[a-zA-Z_][a-zA-Z0-9_]*$`.

A value without `*` or `|` uses exact equality. A value containing either
selects an anchored regular-expression path, not a shell glob. For example,
`W0A0|W0A1` matches either complete value; `wxt.*` matches names starting with
`wxt`. A dot in an exact value such as `wxt.env.temp` stays literal; in regex
mode it has regex meaning. Hot and cold use different regex engines, and a
shared validator restricts accepted strings. Use simple patterns and do not
assume arbitrary regex syntax is supported across both tiers.

### Backend support

| Capability | Hot only | Cold only | Spans watermark |
| --- | --- | --- | --- |
| Ordinary filtered extract | Yes | Yes | Yes, cold then hot |
| Absolute bounds | Yes | Yes | Required |
| Direct relative bounds | Supported by hot-only service | No | No, including hot ranges in tiered mode |
| `head` or `tail` | Yes, per Flux table | Yes, per series | Rejected |
| Aggregation | Yes | Rejected | Rejected |
| Bucket override | Yes | Ignored | Applies only to hot part |

Without a window, supported hot functions are `mean`, `min`, `max`, `sum`,
and `count`. With a window, the function and interval are passed to Flux's
`aggregateWindow`; Flux performs further validation. A window without a
function is rejected. Buckets beginning with `_` are rejected by the hot backend.

Cold SQL explicitly orders by timestamp. Hot results retain Flux's iteration
order; do not assume a global timestamp sort across every hot series. The
router concatenates the disjoint cold and hot ranges. Ordering among rows
with equal timestamps is not a stable tie-break contract.

## NDJSON response

NDJSON is the default when the request does not accept Arrow. Each complete
line contains one record with `timestamp`, `name`, `value`, and `meta`.

```sh
curl --fail-with-body --silent --show-error \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/x-ndjson' \
  --data-binary '{"start":"2025-03-01T00:00:00Z","end":"2025-03-01T00:01:00Z","filter":{"name":"wxt.env.temp","vsn":"W0A0"}}' \
  http://localhost:10000/api/v1/query > measurements.ndjson
```

Example record (illustrative metadata):

```json
{"timestamp":"2025-03-01T00:00:00.026403194Z","name":"wxt.env.temp","value":11.7,"meta":{"vsn":"W0A0"}}
```

`timestamp` is an RFC3339 JSON string, not an integer epoch. `value` is a native
JSON value, not the Arrow field's JSON text. Metadata is an open tag object;
nullable metadata can occur in the shared record representation. Cold JSON
values retain integer digits rather than passing through float64 conversion.
Use an exact-integer decoder when that distinction matters.

The handler names downloads `sage-download-<timestamp>.ndjson`. It does not
explicitly set an NDJSON media type; do not depend on receiving
`application/x-ndjson` in the response header. The service does not implement
HTTP gzip compression for this path.

## Arrow response

Request `Accept: application/vnd.apache.arrow.stream`. The response is an IPC
**stream**, not an Arrow file or Parquet file.

| Field | Arrow type | Meaning |
| --- | --- | --- |
| `timestamp` | `timestamp[ns]`, no timezone annotation | UTC instant |
| `name` | UTF-8 string | Measurement name |
| `value` | UTF-8 string | JSON text for the measurement value |
| `meta` | UTF-8 string | JSON text for the tag object |

The schema permits nulls. Decode non-null `value` and `meta` strings as JSON.
For example, value text `11.7` decodes to a number, while text `"image.jpg"`
decodes to a string. JSON integer text such as `9007199254740993` retains all
digits. Stored VARIANT and MAP types are not exposed as Arrow wire fields.
Hot, cold, and combined responses use this same schema.

Arrow responses set `Content-Type: application/vnd.apache.arrow.stream`,
`X-Arrow-Compression: zstd|lz4|none`, and a `.arrows` download name. Zstd is the
default. Compression applies to IPC buffers, not HTTP `Content-Encoding`.
The codec is set by the operator, not a per-request header.

```sh
curl --fail-with-body --silent --show-error \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/vnd.apache.arrow.stream' \
  --data-binary '{"start":"2025-03-01T00:00:00Z","end":"2025-03-01T00:01:00Z","filter":{"name":"wxt.env.temp","vsn":"W0A0"}}' \
  http://localhost:10000/api/v1/query > measurements.arrows
```

Decode batches with PyArrow, which handles IPC compression:

```python
import json
import pyarrow.ipc as ipc

with open("measurements.arrows", "rb") as stream:
    with ipc.open_stream(stream) as reader:
        for batch in reader:
            values = [json.loads(v) if v is not None else None
                      for v in batch.column("value").to_pylist()]
            metadata = [json.loads(v) if v is not None else None
                        for v in batch.column("meta").to_pylist()]
            print(batch.num_rows, values[:1], metadata[:1])
```

### Sage Data Client

With the matching client installed, `query()` negotiates Arrow, falls back to
NDJSON according to the response media type, and returns a pandas dataframe.
A released client may differ from the workspace implementation.

```python
import sage_data_client

frame = sage_data_client.query(
    start="2025-03-01T00:00:00Z",
    end="2025-03-01T00:01:00Z",
    filter={"name": "wxt.env.temp", "vsn": "W0A0"},
    endpoint="http://localhost:10000/api/v1/query",
)
print(frame[["timestamp", "name", "value", "meta.vsn"]].head())
```

Metadata becomes `meta.<key>` columns. The client retains nanosecond bounds
and resolves relative inputs such as `-1h` before sending absolute timestamps.
It still builds a complete dataframe, so batch decoding does not eliminate
memory needed for the final result.

## Errors and partial results

Errors use plain text, not a structured JSON error envelope.

| Situation | Current behavior |
| --- | --- |
| Empty body, malformed JSON, unknown field, missing start, invalid filter key | HTTP 400 |
| Request exceeds 4096 bytes | HTTP 400; message incorrectly says “must be <1KB” |
| Backend startup/query error, invalid backend filter/time, unsupported cold operation | HTTP 500 |
| Arrow encoding/fetch failure after headers | HTTP framing is aborted; clients must treat it as failure |
| NDJSON iteration failure after headers | Error is logged; a partial response can finish without a distinct HTTP error |

HTTP 200 and valid individual rows do not establish completeness for NDJSON.
When completeness matters, verify expected counts/content independently.
Large cold queries may wait for scan and sort before producing their first
batch; an open connection alone is not evidence of a stall.

## Live SSE measurements

The SSE endpoint uses RabbitMQ, independently of stored-query routing.

```sh
curl --get --no-buffer --fail-with-body \
  --data-urlencode 'name=wxt.env.temp' \
  --data-urlencode 'vsn=W0A0' \
  http://localhost:10000/api/v0/stream
```

Responses use `text/event-stream` and emit:

```text
event: message
data: {"name":"wxt.env.temp","timestamp":"2025-03-01T00:00:00Z","value":11.7,"meta":{"vsn":"W0A0"}}

```

Idle keepalives are SSE comments: `:keepalive` followed by a blank line. The
heartbeat defaults to 15 seconds and resets when broker messages arrive.
The connection ends when the client disconnects; no replay cursor is provided.

SSE filters differ from stored queries: `name` selects RabbitMQ topics,
splitting alternatives on `|`; when omitted it subscribes to `#`. Other query
parameters become Go regex matches against metadata, without automatic
anchoring. The handler uppercases the `vsn` filter and lowercases `node`.
There is no `start`/`end` history selection on this endpoint.

Non-GET stream requests return 405, invalid regex filters return 400, and
broker setup failures return 502. Query and stream responses enable
`Access-Control-Allow-Origin: *` once response setup succeeds; this is not a
complete browser preflight implementation.

Source references: [request/response handler](../internal/httpapi/query.go),
[Arrow schema and encoder](../internal/arrowio/response.go), and
[SSE handler](../internal/httpapi/stream.go).
