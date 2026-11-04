# Architecture

[Documentation index](README.md)

The Go module is `beehive-data-api`. The root entrypoint constructs dependencies and routes; implementation lives in internal packages.

## Packages and dependencies

| Package | Responsibility |
| --- | --- |
| `internal/query` | Query and record types, backend/result interfaces, shared filter validation |
| `internal/arrowio` | Arrow schema, request intent, IPC encoding and compression |
| `internal/backend/hot` | Influx/Flux query generation and record conversion |
| `internal/backend/cold` | Iceberg SQL, native DuckDB execution, JSON scanning, Pelican signing and profiles |
| `internal/backend/tiered` | Static/dynamic frontier routing, read coordination, renewable leases and composition of disjoint time ranges |
| `internal/httpapi` | Query parsing, response negotiation, HTTP/SSE handlers and metrics |
| `internal/testutil` | In-memory fixtures imported only by tests |

The shared query package has no Arrow or database dependencies. Arrow encoding uses query contracts and imports no backend. The tiered backend uses query contracts and imports neither concrete store. Cold storage uses Arrow helpers without importing the HTTP handler in production; its integration tests can exercise the real HTTP handler.

```mermaid
flowchart TD
    Main[main] --> HTTP[httpapi]
    Main --> Hot[backend/hot]
    Main --> Cold[backend/cold]
    Main --> Tiered[backend/tiered]
    Main --> Arrow[arrowio]
    Main --> Query[query contracts]
    HTTP --> Arrow[arrowio]
    HTTP --> Query[query contracts]
    Cold --> Arrow
    Arrow --> Query
    Cold --> Query
    Hot --> Query
    Tiered --> Query
```

## Request execution

A submitted query is parsed, routed by the configured backend, and then written as NDJSON or Arrow. Arrow intent is recorded before `backend.Query`, so the cold backend can choose its native stream. The handler closes the returned result.

```mermaid
flowchart TD
    Submit["POST /api/v1/query"] --> Limit["Read the body, 4 KB limit"]
    Limit --> Parse["Parse JSON, require start, check filter keys"]
    Parse --> Accept{"Accept selects Arrow?"}
    Accept -->|yes| Intent["Record Arrow intent"]
    Accept -->|no| Plain["Keep NDJSON"]
    Intent --> Call["backend.Query"]
    Plain --> Call

    Call --> Mode{"Cold warehouse configured?"}
    Mode -->|no| Hot["Hot backend"]
    Mode -->|yes| Frontier{"COLD_FRONTIER"}
    Frontier -->|static| Static["Use configured COLD_BORDER"]
    Frontier -->|dynamic| Grant["Acquire pinned frontier/snapshot and start lease renewal"]
    Static --> Bounds["Parse absolute start and end"]
    Grant --> Bounds
    Bounds --> Compare{"Range against the border"}
    Compare -->|"ends at or before"| Cold["Cold backend"]
    Compare -->|"starts at or after"| Hot
    Compare -->|crosses| Shape{"Aggregation or head/tail?"}
    Shape -->|yes| Reject["Reject the query"]
    Shape -->|no| Split["Split at the border"]
    Split --> ColdHalf["Cold backend, older half"]
    Split --> HotHalf["Hot backend, newer half"]
    ColdHalf --> Ordered["Composite result, cold then hot"]
    HotHalf --> Ordered

    Cold --> Token["Load the signing key and mint a scoped token"]
    ColdHalf --> Token
    Token --> SQL["Use pinned metadata or resolve the static hint; build SQL"]
    SQL --> ColdFmt{"Arrow intent?"}
    ColdFmt -->|yes| Native["Native DuckDB stream on the reused engine"]
    ColdFmt -->|no| Rows["database/sql scan into records"]

    Hot --> Flux["Build Flux and query Influx"]
    HotHalf --> Flux

    Native --> Encode["Handler encodes the result"]
    Rows --> Encode
    Flux --> Encode
    Ordered --> Encode
    Encode --> Out{"Arrow intent?"}
    Out -->|yes| IPC["WriteIPC: child parts, native batches, or 65,536-row batches"]
    Out -->|no| NDJSON["One NDJSON object per record"]
    IPC --> Close["Close result resources; stop renewal and release any read grant"]
    NDJSON --> Close
```

The query handler validates the JSON request and records Arrow intent in the request context when `Accept` selects Arrow. It calls the configured backend before choosing the response writer. Hot-only operation uses Influx directly; setting a cold warehouse wraps both stores in the tiered router.

The router sends ranges ending at or before the border to cold storage, and ranges starting at or after it to hot storage. Ordinary crossing requests are split at the exact nanosecond boundary, with cold results emitted first. Crossing head/tail and aggregation requests are rejected rather than combined with incorrect per-series semantics. Static routing trusts operator-configured coverage. Dynamic routing also checks the coordinator's coverage start, warehouse and pinned metadata URL. The API reads Influx/Pelican directly; coordination does not proxy query results.

The hot backend generates Flux, reads records through the Influx SDK, and extracts measurement values and tags. The HTTP encoder converts those records to NDJSON or batches of up to 65,536 rows for Arrow. Hot iteration order comes from Flux, including its table grouping.

## Cold execution

Each cold request loads its signing key and mints a scoped read token. Static routing resolves `version-hint.text`; dynamic routing supplies a pinned URL. The resulting metadata-file URL is pinned for that request. DuckDB reads Iceberg manifests and selected Parquet objects directly; there is no API-side file-list cache or catalog registration step.

Both formats use the same SQL source predicates, absolute half-open time bounds and optional head/tail logic. NDJSON uses a database/sql projection and scanner; Arrow uses the native DuckDB streaming API and returns Arrow batches directly. The public projection serializes the stored value to JSON text. Ordinary metadata projection uses checked JSON fast paths with a general fallback; head/tail retain a merged MAP for per-series partitioning.

The backend holds its lock until results close, so both formats share a serialized cold resource budget and cannot replace each other's database-wide scoped secret mid-query. The native Arrow database is initialized lazily and reused. Connections, results and scoped request setup remain request-owned.

Native streaming avoids retaining a fully materialized result outside the DuckDB query engine. It does not remove blocking scan, projection and sort work. Queries execute over the whole range with an explicit final timestamp sort; large requests may spill before their first batch. No chronological execution-window or uniform-metadata discovery path remains.

## Result ownership

`query.Backend.Query` returns `query.Results` with `Next`, `Record`, `Err` and `Close`. The HTTP handler closes the returned result. Ordinary hot/NDJSON results are iterated row by row. Native cold results additionally expose an Arrow reader through a structural interface recognized by the encoder.

Tiered results implement `query.CompositeResults.Parts()` to expose ordered child streams without revealing their concrete result type. The encoder stops on a child error and does not close children itself. Closing the enclosing tiered result releases both children; opening hot unsuccessfully also closes the already-open cold result.

`arrowio.WriteReader` borrows the supplied record reader and writes batches using the public schema; the caller retains responsibility for releasing the reader. Schema adoption can reuse column buffers with reference counting. The native cold reader releases its connection/result resources before the backend lock is released. Cancellation and error paths follow the same ownership boundaries.

## Cold storage contract

The current reader targets Iceberg v3 observations with:

- `time` stored as `TIMESTAMPTZ_NS`, preserving nanosecond instants;
- `name` as the measurement name and one `value VARIANT` column;
- `meta MAP(VARCHAR, VARCHAR)` for metadata;
- promoted tag columns `plugin`, `vsn`, `node`, `host`, `task`, `job`, and `zone`.

Promoted tags are merged into public metadata with the existing collision precedence. Stored types are projected into the same HTTP schema as hot storage. NDJSON values retain raw JSON text internally instead of decoding large integers through float64. Arrow exposes JSON strings, not storage MAP or VARIANT arrays.

For the deployed URL layout, the warehouse base is `https://<host>/sage/<isolated-prefix>/warehouse`. The API reads `<base>/sage/obs/metadata/version-hint.text` in static mode, validates the metadata stem and opens `<base>/sage/obs/metadata/<stem>.metadata.json`. Dynamic mode validates and opens the coordinator's pinned metadata URL instead. Referenced metadata, manifests and data objects must remain accessible. The API is a reader; it does not publish snapshots or write warehouse files.

The adopted writer policy uses day partitions, chronological file batches of at most 10 million rows, rows sorted by plugin then vsn then time within a file, and plugin-pure row groups capped at 262,144 rows. Large plugins span multiple groups; small plugins can share files. This is warehouse writer policy, not a geometry constraint validated by the API on each request.

## Dynamic archival integration

The tiered backend supports both static and dynamic frontiers. Dynamic deployments configure its `Coordinator` to pin the cold frontier and metadata URL before tier routing. The read grant remains held through result closure, with a renewable lease supplied by the coordinator. Background renewal preserves the pinned view and allows healthy long queries to continue. Lease loss cancels the query; unreleased grants stop blocking deletion after the last confirmed expiry and a one-minute drain grace. Cold execution scans the pinned URL instead of resolving a newer hint mid-request. The ownership wrapper exposes child results so native Arrow batches continue through the existing encoder.

Configuration parsing, the shared router, the coordinator client and lease renewal all live in `internal/backend/tiered`. `Backend.Border` supplies the static boundary; configuring `Backend.Coordinator` selects dynamic routing. A per-request boundary is passed to the shared routing function without mutating backend configuration. `query.WithColdMetadata` carries the pinned snapshot. There is one tiered backend for both frontier modes.

Result closure stops renewal and cancels the query context, then closes the underlying resources. Successful closure releases the grant once; uncertain closure leaves it to expire automatically. A lost acquire response can also leave a grant until expiry. Lease expiry allows deletion progress despite uncertain closure; it does not prove all remote work has terminated. Keep host clocks synchronized. Leases do not renew Pelican credentials or override caller, Influx-client or reverse-proxy deadlines.

This lets the archiver publish and verify a day, activate its cold coverage, then drain old hot readers before deleting that day from Influx. Public response schemas are unchanged. See [configuration](configuration.md#dynamic-vs-static-cold-frontier) and the [archival design](https://github.com/seanshahkarami/sage-data-rework/blob/main/plans/12_dynamic_archival_service.md).
