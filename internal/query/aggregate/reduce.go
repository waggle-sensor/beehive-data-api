// reduce.go groups equivalent series before buffering observations.
// Full metadata is rebuilt only for the reduced output.

package aggregate

import (
	model "beehive-data-api/internal/query"
	"fmt"
	"strings"
	"time"
)

// One numeric reduction per canonical series/window preserves native arithmetic.
// Full metadata is reconstructed only for the reduced output, not every row.
func buildReducedAggregateSQL(query *model.Query, source string, now time.Time) (string, error) {
	if query.Head != nil || query.Tail != nil {
		return "", fmt.Errorf("observation limits must be applied before reduction")
	}
	start, end, err := Bounds(query, now)
	if err != nil {
		return "", err
	}
	bucket, boundaries, n, err := aggregateBoundaries(query, start, end)
	if err != nil {
		return "", err
	}
	value := aggregateValueSQL(*query.Func)
	var tags, projected, groupKeys, publicKeys, publicValues []string
	for _, tag := range promotedTags {
		tags = append(tags, sqlQuote(tag))
		projected = append(projected, "coalesce("+tag+",meta["+sqlQuote(tag)+"]) AS key_"+tag,
			"("+tag+" IS NOT NULL OR coalesce(list_contains(map_keys(meta),"+sqlQuote(tag)+"),false)) AS has_"+tag)
		groupKeys = append(groupKeys, "key_"+tag, "has_"+tag)
		publicKeys = append(publicKeys, "CASE WHEN has_"+tag+" THEN "+sqlQuote(tag)+" ELSE NULL END")
		publicValues = append(publicValues, "key_"+tag)
	}
	residual := "list_sort(list_filter(map_entries(COALESCE(meta,MAP {}::MAP(VARCHAR,VARCHAR))),lambda x:x.key NOT IN (" + strings.Join(tags, ",") + ")))"
	promoted := "map_from_entries(list_filter(list_zip([" + strings.Join(publicKeys, ",") + "],[" + strings.Join(publicValues, ",") + "]),lambda x:x[1] IS NOT NULL))"
	meta := "map_from_entries(list_sort(map_entries(map_concat(map_from_entries(residual_keys)," + promoted + "))))"
	output := "a.v"
	if *query.Func == "count" {
		output = "coalesce(a.v,0::BIGINT::VARIANT)"
	}
	join := "SELECT s.name,s.meta,w.stop_ns," + output + " AS v FROM series s CROSS JOIN boundaries w LEFT JOIN reduced a ON a.name=s.name AND a.meta=s.meta AND a.bucket=w.bucket"
	if query.Window == nil && (*query.Func == "min" || *query.Func == "max") {
		join += " WHERE a.n IS NOT NULL"
	}
	return fmt.Sprintf(`WITH input AS NOT MATERIALIZED (%s),
 keyed AS NOT MATERIALIZED (SELECT timestamp,name,value,%s AS kind,%s AS residual_keys,%s FROM input),
 partial_states AS MATERIALIZED (SELECT name,residual_keys,%s,%s AS bucket,count(*) AS n,%s AS v FROM keyed GROUP BY name,residual_keys,%s,bucket),
 reduced AS MATERIALIZED (SELECT name,bucket,n,v,%s AS meta FROM partial_states),
 series_raw AS (SELECT DISTINCT name,meta FROM reduced),
 series AS (SELECT * FROM series_raw WHERE (SELECT CASE WHEN count(*)>%d THEN error('aggregation result expansion exceeds budget') ELSE true END FROM series_raw)),
 boundaries AS (%s),output AS (%s)
 SELECT make_timestamp_ns(stop_ns) AS timestamp,name,to_json(v)::VARCHAR AS value,to_json(meta)::VARCHAR AS meta FROM output ORDER BY timestamp,name,meta`,
		source, aggregateKind("value"), residual, strings.Join(projected, ","), strings.Join(groupKeys, ","), bucket, value, strings.Join(groupKeys, ","), meta, maxAggregateRows/n, boundaries, join), nil
}
