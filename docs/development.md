# Development

[Documentation index](README.md) · [Architecture](architecture.md)

The module is `beehive-data-api`. Keep the root entrypoint focused on startup
wiring. Implement changes in the owning internal package and keep unit tests
beside it. Use `internal/testutil` for shared in-memory fixtures instead of
importing private helpers from another package's tests.

## Build requirements

Use a Go version satisfying `go.mod` (currently Go 1.24.0 minimum), a working
CGO C/C++ toolchain, and module/extension download access when caches are empty.
Docker builds use Go 1.26 on Debian Bookworm. Native DuckDB is required by the
service even when cold routing is disabled in its runtime configuration.

DuckDB 2.0 still gates its `arrowmapping` dependency on `duckdb_arrow`. Always
include the tag in supported service build, test and vet commands. Removing
the application's old error stub did not remove the dependency requirement.

From the repository root:

```sh
CGO_ENABLED=1 go build -tags=duckdb_arrow -o beehive-data-api .
CGO_ENABLED=1 go test -tags=duckdb_arrow ./...
CGO_ENABLED=1 go vet -tags=duckdb_arrow ./...
```

CI runs build/test/vet with that tag and CGO enabled. The Dockerfile builds the
same implementation. An untagged build is not a supported fallback.
Do not substitute a cached different Arrow or DuckDB version in repository
module files to claim the pinned build passed.

## Tests by package

```sh
CGO_ENABLED=1 go test -tags=duckdb_arrow ./internal/arrowio ./internal/httpapi
CGO_ENABLED=1 go test -tags=duckdb_arrow ./internal/backend/hot ./internal/backend/tiered
CGO_ENABLED=1 go test -tags=duckdb_arrow ./internal/backend/cold
```

Existing coverage includes public schema across tiers, compression,
metadata/null/value semantics, nanosecond bounds, routing, native streaming,
cancellation, error propagation, profiles and result cleanup. Cold native tests
can need extension downloads even when not using a live warehouse. Tests
explicitly requiring a REST fixture or probe inputs skip unless configured.

Preserve the ownership contract: the handler closes the enclosing result;
composite encoding borrows ordered parts, and `arrowio.WriteReader` borrows a
record reader without releasing it. Check failure and cancellation paths when
changing anything that holds the cold backend lock.

## Native Iceberg v3 fixture

`TestIcebergV3RoundTrip` writes a fresh table through DuckDB's Iceberg REST
writer and reads the committed metadata directly. It exercises nanoseconds,
VARIANT values, HTTP Arrow/NDJSON, head/tail and tiered responses.

It requires a separately provisioned **isolated** REST catalog. The local
warehouse path must be mounted at the same absolute path inside that catalog.
These commands assume that fixture is already running; they do not provision it:

```sh
export COLD_V3_ENDPOINT='http://127.0.0.1:18186'
export COLD_V3_ROOT='/absolute/path/to/fresh-isolated-test-warehouse'
CGO_ENABLED=1 go test -tags=duckdb_arrow -count=1 \
  -run '^TestIcebergV3RoundTrip$' ./internal/backend/cold
```

Do not point test writers at a published warehouse or `/node-data`. The test
skips if either variable is unset. `COLD_V3_HTTP_FIXTURE_DIR` optionally exports
actual HTTP response fixtures into a separate fresh directory for replay by
the matching Sage Data Client tests. It is an export location, not the catalog
warehouse. Fixture export refuses to overwrite existing response files.

For an ordinary suite run, unset stale integration/export variables:

```sh
env -u COLD_V3_ENDPOINT -u COLD_V3_ROOT -u COLD_V3_HTTP_FIXTURE_DIR \
  CGO_ENABLED=1 go test -tags=duckdb_arrow ./...
```

The broader workspace contains optional REST-fixture tooling; the repository
can still run its non-REST coverage without that sibling directory.

## Experiment probes

Root `main` has no tests. Compile each probe from its owning package:

```sh
CGO_ENABLED=1 go test -tags=duckdb_arrow -c \
  -o /tmp/sage-cold-probe.test ./internal/backend/cold
CGO_ENABLED=1 go test -tags=duckdb_arrow -c \
  -o /tmp/sage-arrow-probe.test ./internal/arrowio
```

| Test entrypoint | Binary | Inputs / outputs |
| --- | --- | --- |
| `TestColdExecutionProbe` | cold | `COLD_EXECUTION_QUERIES` JSON cases and `COLD_EXECUTION_PROBE_DIR`; exports baseline/current SQL |
| `TestColdProjectionProbe` | cold | `COLD_PROJECTION_QUERIES` and `COLD_PROJECTION_PROBE_DIR`; exports projection SQL |
| `TestColdDuckDBEvaluationProbe` | cold | `COLD_EVAL_SQL` and `COLD_EVAL_DIR`; executes native SQL and writes Arrow/timing outputs |
| `TestArrowCompressionProbe` | Arrow | `ARROW_PROBE_INPUT`, `ARROW_PROBE_OUTPUT`, `ARROW_PROBE_MODE`, `ARROW_PROBE_REPORT`; encodes a current-schema IPC fixture |

These are opt-in test harnesses, not HTTP endpoints. Use unique local output
folders and read the owning test before supplying paths. For example, list
the compiled entrypoints without running them:

```sh
/tmp/sage-cold-probe.test -test.list 'TestCold.*Probe'
/tmp/sage-arrow-probe.test -test.list '^TestArrowCompressionProbe$'
```

The cold memory probe uses `arrowio.WriteReader`; it is not a private cold
writer. Test-only SQL baselines stay in the cold package's `_test.go` files.
Historical split-value and uniform-metadata exporters were removed with their
rejected prototypes; old result reports do not imply those entrypoints exist.

## Validation practice

Check formatting, the pinned tagged suite, vet and the image build for changes
that affect execution or packaging. Add regression coverage for actual behavior
and ownership changes rather than tests that mirror implementation details.
For docs-only edits, validate commands, examples and links without starting
services or writing warehouse data.

Record which checks passed, skipped or could not run. An isolated build with
substitute cached dependencies is useful diagnostic evidence, but is not a
successful pinned/native build. Run the full suite in an environment with the
required dependencies before considering that validation complete.
