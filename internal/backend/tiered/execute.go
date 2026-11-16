// execute.go stages the hot half of a spanning aggregate or head/tail query,
// then runs one cold SQL plan over the raw union. The private Parquet
// directory lives until that result closes successfully.

package tiered

import (
	"beehive-data-api/internal/backend/tiered/stage"
	model "beehive-data-api/internal/query"
	"beehive-data-api/internal/query/aggregate"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// defaultAggregateStagingBytes is 16 GiB of uncompressed JSON-equivalent input.
const defaultAggregateStagingBytes int64 = 16 << 30

// UnionSource preserves raw tuples and exact values for the final cross-tier operation.
func UnionSource(source, path string) string {
	value := `CASE kind WHEN 'int' THEN value::BIGINT::VARIANT WHEN 'uint' THEN value::UBIGINT::VARIANT WHEN 'float' THEN value::DOUBLE::VARIANT WHEN 'bool' THEN value::BOOLEAN::VARIANT WHEN 'string' THEN value::VARCHAR::VARIANT WHEN 'null' THEN NULL::VARIANT ELSE error('invalid hot value kind') END`
	// Hot metadata is already merged. Null promoted columns leave it intact
	// when the shared dictionary applies the cold collision rules.
	nulls := make([]string, len(aggregate.PromotedTags()))
	for i := range nulls {
		nulls[i] = "NULL::VARCHAR"
	}
	source += " UNION ALL SELECT make_timestamp_ns(time),name,meta," + strings.Join(nulls, ",") + "," + value + " FROM read_parquet(" + aggregate.Quote(path) + ",hive_partitioning=false)"
	return source
}

// stageHotQuery writes the hot slice to private Parquet and plans the cold
// union over that file. The caller's function, window, and limits are removed
// from each tier read; they belong to the final statement only.
func (backend *Backend) stageHotQuery(ctx context.Context, query *model.Query, border, end time.Time) (result model.Results, err error) {
	executor, ok := backend.Cold.(model.PlannedSourceBackend)
	if !ok {
		if query.Func != nil {
			return nil, fmt.Errorf("cold backend does not provide raw-source execution for spanning aggregation")
		}
		return nil, fmt.Errorf("cold backend does not provide raw-source execution for spanning per-series head/tail selection")
	}
	ctx, profile := executor.BeginQueryProfile(ctx)
	defer func() {
		if err != nil {
			profile.Finish(true)
		}
	}()
	if query.End == "" {
		resolved := *query
		resolved.End = end.UTC().Format(time.RFC3339Nano)
		query = &resolved
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = executor.ValidateSourceBucket(query); err != nil {
		return nil, err
	}
	reader, ok := backend.Hot.(model.RawSourceBackend)
	if !ok {
		return nil, fmt.Errorf("hot backend cannot validate staged input fields")
	}
	dir, err := os.MkdirTemp(backend.TieredStagingDir, "sage-aggregate-")
	if err != nil {
		return nil, err
	}
	owned := true
	defer func() {
		if owned {
			os.RemoveAll(dir)
		}
	}()
	raw := *query
	raw.Start = border.UTC().Format(time.RFC3339Nano)
	raw.Func = nil
	raw.Window = nil
	raw.Head = nil
	raw.Tail = nil
	phaseStart := profile.Clock()
	rows, err := reader.QueryRawSource(ctx, &raw)
	profile.Duration("aggregate_hot_open_seconds", phaseStart)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, fmt.Errorf("hot source returned no result stream")
	}
	parquet := filepath.Join(dir, "hot.parquet")
	phaseStart = profile.Clock()
	if err = backend.writeHotStage(ctx, rows, parquet, &raw); err != nil {
		return nil, err
	}
	profile.Duration("aggregate_hot_read_stage_seconds", phaseStart)
	if profile.Enabled() {
		if stat, statErr := os.Stat(parquet); statErr == nil {
			profile.Value("aggregate_parquet_bytes", float64(stat.Size()))
		}
	}
	profile.Mark("aggregate_staging_complete_since_backend_start_seconds")
	coldQuery := *query
	coldQuery.End = border.UTC().Format(time.RFC3339Nano)
	coldQuery.Func, coldQuery.Window, coldQuery.Head, coldQuery.Tail = nil, nil, nil, nil
	result, err = executor.QuerySource(ctx, &coldQuery, query.Func != nil, func(source string, now time.Time) (string, error) {
		source = UnionSource(source, parquet)
		if query.Func != nil {
			return aggregate.BuildSQL(query, source, now)
		}
		return BuildSelectionSQL(query, source, now)
	})
	if err != nil {
		return nil, err
	}
	owned = false
	return &stagedResults{Results: result, dir: dir}, nil
}

// primitiveScalar records the Go kind and the exact text staged for that scalar.
// A non-finite float is rejected. The cold union casts the text back to VARIANT.
func primitiveScalar(raw any) (kind, text string, err error) {
	switch value := raw.(type) {
	case nil:
		return "null", "", nil
	case float64:
		if math.IsInf(value, 0) || math.IsNaN(value) {
			return "", "", fmt.Errorf("nonfinite hot aggregation value")
		}
		return "float", strconv.FormatFloat(value, 'g', -1, 64), nil
	case int64:
		return "int", strconv.FormatInt(value, 10), nil
	case uint64:
		return "uint", strconv.FormatUint(value, 10), nil
	case bool:
		return "bool", strconv.FormatBool(value), nil
	case string:
		return "string", value, nil
	default:
		return "", "", fmt.Errorf("unsupported hot aggregation value %T", raw)
	}
}

// writeHotStage writes the sole hot aggregation staging format: private Parquet.
// It closes the hot source before the caller starts native cold execution.
func (backend *Backend) writeHotStage(ctx context.Context, rows model.Results, path string, query *model.Query) (err error) {
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, model.ErrResourceClosureUncertain, closeErr)
		}
	}()
	stagedFile, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, stagedFile.Close()) }()
	max := backend.TieredMaxStagingBytes
	if max == 0 {
		max = defaultAggregateStagingBytes
	}
	if max < 1 {
		return fmt.Errorf("aggregation staging byte budget must be positive")
	}
	writer, err := stage.New(stagedFile, max)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, writer.Close()) }()
	profile := model.QueryProfileFromContext(ctx)
	var stagedRows int64
	start, _ := time.Parse(time.RFC3339Nano, query.Start)
	end, _ := time.Parse(time.RFC3339Nano, query.End)
	for rows.Next() {
		if err = ctx.Err(); err != nil {
			return err
		}
		r := rows.Record()
		// A Flux range that starts at the Unix epoch can still return an
		// earlier point. That point is outside this hot slice, so it is left
		// to the cold half instead of being staged or failing the request.
		if r != nil && r.Timestamp.Before(start) {
			continue
		}
		if r == nil || !r.Timestamp.Before(end) || !r.Timestamp.Equal(time.Unix(0, r.Timestamp.UnixNano())) {
			return fmt.Errorf("hot aggregate input outside requested nanosecond range")
		}
		kind, value, scalarErr := primitiveScalar(r.Value)
		if scalarErr != nil {
			return scalarErr
		}
		meta := r.Meta
		if meta == nil {
			meta = map[string]string{}
		}
		if err = writer.Append(stage.Row{Time: r.Timestamp.UnixNano(), Name: r.Name, Meta: meta, Kind: kind, Value: value}); err != nil {
			return err
		}
		stagedRows++
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = writer.Close(); err != nil {
		return err
	}
	if profile.Enabled() {
		profile.Value("aggregate_hot_rows", float64(stagedRows))
		profile.Value("aggregate_input_bytes", float64(writer.InputBytes()))
	}
	return nil
}

// stagedResults deletes the private hot Parquet directory after the child
// stream closes successfully. A close error leaves the directory in place.
type stagedResults struct {
	model.Results
	dir  string
	once sync.Once
	err  error
}

// Parts exposes the child stream without transferring ownership of its files.
func (r *stagedResults) Parts() []model.Results { return []model.Results{r.Results} }

// Close closes the child once and removes the staging directory only when
// that close succeeds. A later close returns the same error.
func (r *stagedResults) Close() error {
	r.once.Do(func() {
		r.err = r.Results.Close()
		if r.err == nil {
			r.err = os.RemoveAll(r.dir)
		}
	})
	return r.err
}
