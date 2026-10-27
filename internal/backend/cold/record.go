// record.go contains the cold record functions.
package cold

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	model "beehive-data-api/internal/query"
)

// The v3 SQL projection returns JSON text for the single VARIANT value.
// Retain it as RawMessage: decoding through float64 would corrupt large ints.
func scanColdRecord(rows *sql.Rows) (*model.Record, error) {
	var ts time.Time
	var name string
	var value sql.NullString
	var metadata any
	if err := rows.Scan(&ts, &name, &value, &metadata); err != nil {
		return nil, err
	}
	raw, err := coldValueJSON(value)
	if err != nil {
		return nil, err
	}
	meta, err := coldMeta(metadata)
	if err != nil {
		return nil, err
	}
	return &model.Record{Timestamp: ts.UTC(), Name: name, Value: raw, Meta: meta}, nil
}

func coldValueJSON(value sql.NullString) (json.RawMessage, error) {
	if !value.Valid {
		return json.RawMessage("null"), nil
	}
	if !json.Valid([]byte(value.String)) {
		return nil, fmt.Errorf("cold value cannot be represented as JSON: %q", value.String)
	}
	return json.RawMessage(value.String), nil
}

// coldMeta accepts either form DuckDB hands back: a JSON string, or the map
// the driver already decoded. A missing map is an empty object, because
// sage-data-client indexes meta without a nil check.
func coldMeta(v any) (map[string]string, error) {
	if v == nil {
		return map[string]string{}, nil
	}
	switch m := v.(type) {
	case map[string]string:
		return m, nil
	case map[string]any:
		out := make(map[string]string, len(m))
		for k, val := range m {
			if val == nil {
				continue
			}
			s, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("meta[%s] is %T", k, val)
			}
			out[k] = s
		}
		return out, nil
	case string:
		return coldMetaJSON(m)
	case []byte:
		return coldMetaJSON(string(m))
	default:
		return nil, fmt.Errorf("meta type %T", v)
	}
}

// coldMetaJSON unmarshals a JSON string into a map[string]string.
func coldMetaJSON(s string) (map[string]string, error) {
	if s == "" || s == "null" {
		return map[string]string{}, nil
	}
	out := map[string]string{}
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, fmt.Errorf("meta: %w", err)
	}
	if out == nil {
		out = map[string]string{}
	}
	return out, nil
}
