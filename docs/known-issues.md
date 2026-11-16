# Known issues

## Flux can omit observations across native value-type changes

**Classification:** upstream InfluxDB/Flux read limitation. The local
investigation was closed on 2026-11-15 with a decision to retain the current
API behavior and accept this known limitation. Closing the investigation does
not mean the upstream behavior is fixed.

Observed with InfluxDB 2.7.7: when one Flux source read spans shards where a
measurement's field changes native type, it can return only one type's
observations. The response can finish with HTTP 200, valid annotated CSV and
no query or transport error, despite omitting stored observations.

The production example was `sys.rssh_up`: earlier `value` observations were
integers, and observations from 2022-02-14 were doubles. A January 31–May 1
whole-range read returned 1,235,246 observations, while the separate daily
reads returned 12,854,934. Direct requests to Influx's `/api/v2/query`
reproduced the same omission without either data API involved. The change
coincided with an actual shard-group boundary.

### Impact

- Affected Flux-backed hot queries can return incomplete input. This includes
  the hot portion of tiered queries and operations that depend on those rows.
- Arrow and NDJSON cannot recover observations omitted by the source read.
  Consuming a response successfully does not prove source completeness.
- The observations remain stored in Influx. This is not evidence of missing
  tags, lost stored values, or extra observations introduced by cold storage.
- Cold-only Iceberg reads do not use Flux and are outside this limitation.

### Investigation and disposition

Independent reads contained within the tested boundaries recovered the
observations with their original types, tags, values and nanosecond timestamps.
Native unions matched the complete daily multiset, including 12,854,934
observations in the name-only test and 1,777,337 across 104 measurements in the
VSN test. Whole-range type filters and conversions did not recover the omitted
observations.

The experimental daily/shard-bounded read strategy is **not adopted in
Beehive Data API**. No automatic slicing, value coercion, shard-metadata
integration or deployment change is made for this issue. The experiments
remain research evidence rather than an API correctness guarantee for every
dataset or value type.

Evidence and the closed investigation are recorded in
[`sage-data-rework/plans/16_canary_failures.md`](https://github.com/seanshahkarami/sage-data-rework/blob/main/plans/16_canary_failures.md#closed-mixed-type-flux-completeness-investigation),
experiments 110–116. Revisit this limitation if an upstream fix is evaluated or
the decision to retain the existing read behavior changes.
