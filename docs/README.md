# Documentation

[Project README](../README.md)

These guides describe the current Beehive Data API source. They do not imply that every deployed API or published Sage Data Client release includes these updates.

## Choose a starting point

| Audience | Reading path |
| --- | --- |
| API consumers | [API](api.md): query examples, response formats, client usage, live events |
| Operators | [Operations](operations.md), then [configuration](configuration.md) |
| Maintainers | [Architecture](architecture.md), then [development](development.md) |

## Guides

- [API](api.md): request fields, filters, tier-specific capabilities, Arrow/NDJSON, errors, SSE.
- [Dynamic vs Static Cold Frontier](configuration.md#dynamic-vs-static-cold-frontier): border selection, snapshot pinning and renewable reader leases.
- [Configuration](configuration.md): all startup flags and environment variables, defaults and fixed engine settings.
- [Architecture](architecture.md): package boundaries, query execution, resource ownership and cold storage contracts.
- [Metrics and Grafana](metrics.md): format-specific latency, query/SSE outcomes, throughput, renewal and dashboard provisioning.
- [Operations](operations.md): local/Docker startup, cold warehouse setup, metrics, profiling and troubleshooting.
- [Development](development.md): build requirements, tests, native fixtures and experiment probes.

Configuration is an operator concern. API callers choose the response format through `Accept`; they do not select the storage tier, engine version, compression codec or I/O pool for each request.
