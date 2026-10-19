package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// ColdBackend reads the Iceberg cold tier with DuckDB.
//
// DuckDB's iceberg extension scans a metadata URL over HTTPS, using a
// bearer token for Pelican. There is no separate query service and no
// Arrow materialization: one statement plans the manifests, prunes files,
// and streams rows. The token is installed as an http secret, which is
// database-global, so queries run one at a time.
type ColdBackend struct {
	DB        *sql.DB
	Warehouse string
	KeyPath   string
	Client    *http.Client

	mu sync.Mutex
}

// buildColdBackend opens DuckDB and loads the httpfs and iceberg extensions.
// The first call downloads those extensions; later connections only LOAD them.
func buildColdBackend(warehouse, keyPath string) (*ColdBackend, error) {
	warehouse = strings.TrimRight(warehouse, "/")
	if warehouse == "" {
		return nil, fmt.Errorf("cold warehouse is empty")
	}
	if _, err := readScope(warehouse); err != nil {
		return nil, err
	}
	if _, err := loadPelicanKey(keyPath); err != nil {
		return nil, fmt.Errorf("pelican key: %w", err)
	}

	connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		ctx := context.Background()
		for _, q := range []string{
			"SET TimeZone='UTC'",
			"SET threads=2",
			"SET memory_limit='3GB'",
			"INSTALL httpfs",
			"LOAD httpfs",
			"INSTALL iceberg",
			"LOAD iceberg",
		} {
			if _, err := execer.ExecContext(ctx, q, nil); err != nil {
				return fmt.Errorf("%s: %w", q, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(2)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return &ColdBackend{
		DB:        db,
		Warehouse: warehouse,
		KeyPath:   keyPath,
		Client:    &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// Query reads version-hint.text, then streams the rows inside the query window.
func (backend *ColdBackend) Query(ctx context.Context, query *Query) (Results, error) {
	if query.Func != nil {
		return nil, fmt.Errorf("aggregations are not supported against cold storage")
	}

	key, err := loadPelicanKey(backend.KeyPath)
	if err != nil {
		return nil, err
	}
	scope, err := readScope(backend.Warehouse)
	if err != nil {
		return nil, err
	}
	// Longer than the API's cold timeout, so a query does not outlive its token.
	tok, err := mintSciToken(key, scope, time.Now(), 20*time.Minute)
	if err != nil {
		return nil, err
	}

	stem, err := backend.readHint(ctx, tok)
	if err != nil {
		return nil, err
	}
	metadataURL := fmt.Sprintf("%s/sage/obs/metadata/%s.metadata.json", backend.Warehouse, stem)
	statement, err := buildColdSQL(query, metadataURL, time.Now().UTC())
	if err != nil {
		return nil, err
	}

	backend.mu.Lock()
	released := false
	release := func() {
		if !released {
			released = true
			backend.mu.Unlock()
		}
	}
	conn, err := backend.DB.Conn(ctx)
	if err != nil {
		release()
		return nil, err
	}
	// The secret is visible to every connection. Hold the lock until the
	// scan finishes so a second query cannot replace the token mid-read.
	if strings.ContainsAny(tok, "'\\") {
		conn.Close()
		release()
		return nil, fmt.Errorf("refusing to interpolate a token that contains a quote")
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE OR REPLACE SECRET pelican (TYPE http, EXTRA_HTTP_HEADERS MAP {'Authorization': 'Bearer %s'})",
		tok)); err != nil {
		conn.Close()
		release()
		return nil, fmt.Errorf("pelican secret: %w", err)
	}
	rows, err := conn.QueryContext(ctx, statement)
	if err != nil {
		conn.Close()
		release()
		return nil, err
	}
	return &coldResults{rows: rows, conn: conn, release: release}, nil
}

// readHint reads the version-hint.text file from the cold storage.
func (backend *ColdBackend) readHint(ctx context.Context, tok string) (string, error) {
	hintURL := backend.Warehouse + "/sage/obs/metadata/version-hint.text"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, hintURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	client := backend.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 256))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("version-hint.text returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	stem := strings.TrimSpace(string(body))
	if !validHintStem(stem) {
		return "", fmt.Errorf("version-hint.text has unexpected contents %q", stem)
	}
	return stem, nil
}

func validHintStem(stem string) bool {
	if stem == "" || len(stem) > 128 || strings.Contains(stem, "..") {
		return false
	}
	for _, r := range stem {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r == '-' || r == '_' || r == '.':
		default:
			return false
		}
	}
	return true
}

// coldResults implements Results for a DuckDB query against cold storage.
type coldResults struct {
	rows    *sql.Rows
	conn    *sql.Conn
	release func()
	record  *Record
	err     error
}

// Err returns the error from the last operation.
func (r *coldResults) Err() error { return r.err }

// Close closes the results and releases the database connection.
func (r *coldResults) Close() error {
	err := r.rows.Close()
	if r.conn != nil {
		if cerr := r.conn.Close(); err == nil {
			err = cerr
		}
	}
	r.release()
	return err
}

// Record returns the current row as a Record.
func (r *coldResults) Record() *Record { return r.record }

// Next advances the results to the next row.
func (r *coldResults) Next() bool {
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			r.err = err
		}
		return false
	}
	rec, err := scanColdRecord(r.rows)
	if err != nil {
		r.err = err
		return false
	}
	r.record = rec
	return true
}

// scanColdRecord scans a row from the cold backend into a Record.
func scanColdRecord(rows *sql.Rows) (*Record, error) {
	var ts time.Time
	var name string
	var vf sql.NullFloat64
	var vi sql.NullInt64
	var vs sql.NullString
	var metaVal any
	if err := rows.Scan(&ts, &name, &vf, &vi, &vs, &metaVal); err != nil {
		return nil, err
	}
	var value any
	switch {
	case vf.Valid:
		value = vf.Float64
	case vi.Valid:
		value = vi.Int64
	case vs.Valid:
		value = vs.String
	}
	meta, err := coldMeta(metaVal)
	if err != nil {
		return nil, err
	}
	return &Record{
		Timestamp: ts.UTC(),
		Name:      name,
		Value:     value,
		Meta:      meta,
	}, nil
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
