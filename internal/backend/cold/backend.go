// backend.go contains the cold backend type and its associated functions.
package cold

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"

	"beehive-data-api/internal/arrowio"
	model "beehive-data-api/internal/query"
)

// Backend is the cold backend type.
// Reads the Iceberg cold tier with DuckDB.
//
// DuckDB's iceberg extension scans a metadata URL over HTTPS, using a
// bearer token for Pelican. A newline-JSON response scans rows through
// database/sql. An Arrow response reads DuckDB record batches directly
// (cold_arrow.go) so those rows are not copied into Go records. The token
// is installed as an http secret, which is database-global, so queries run
// one at a time.
type Backend struct {
	coldArrowState
	DB         *sql.DB
	connector  *duckdb.Connector
	Warehouse  string
	KeyPath    string
	Client     *http.Client
	ProfileDir string
	IOSettings *IOSettings

	mu     sync.Mutex
	closed bool
}

// Close closes the cold backend.
// Waits for active cold results and releases both query engines.
func (backend *Backend) Close() error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.closed {
		return nil
	}
	backend.closed = true
	err := backend.closeArrow()
	if backend.DB != nil {
		err = errors.Join(err, backend.DB.Close())
	}
	return err
}

// NewBackend builds the cold backend.
// Opens DuckDB and loads the httpfs and iceberg extensions.
// The first call downloads those extensions; later connections only LOAD them.
func NewBackend(warehouse, keyPath string, settings IOSettings) (*Backend, error) {
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
		for _, q := range coldDuckDBSetup(&settings) {
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
	var engineVersion string
	if err := db.QueryRow("SELECT version()").Scan(&engineVersion); err != nil {
		db.Close()
		return nil, err
	}
	if !strings.HasPrefix(engineVersion, "v2.0.") {
		db.Close()
		return nil, fmt.Errorf("expected DuckDB 2.0.x, linked %s", engineVersion)
	}
	log.Printf("cold DuckDB %s threads=2 memory_limit=3GB async_threads=%d read_ahead_depth=%d", engineVersion, settings.AsyncThreads, settings.ReadAheadDepth)
	return &Backend{
		DB:         db,
		connector:  connector,
		Warehouse:  warehouse,
		KeyPath:    keyPath,
		Client:     &http.Client{Timeout: 30 * time.Second},
		IOSettings: &settings,
	}, nil
}

// Query queries the cold backend.
// Reads version-hint.text, then streams the rows inside the query window.
func (backend *Backend) Query(ctx context.Context, query *model.Query) (model.Results, error) {
	if query.Func != nil {
		return nil, fmt.Errorf("aggregations are not supported against cold storage")
	}

	var profile *coldQueryProfile
	handedOff := false
	if arrowio.WantsResult(ctx) && backend.ProfileDir != "" {
		var profileErr error
		profile, profileErr = newColdQueryProfile(ctx, backend.ProfileDir)
		if profileErr != nil {
			log.Printf("cold profile creation: %v", profileErr)
		} else {
			ctx = context.WithValue(ctx, coldQueryProfileKey{}, profile)
		}
	}
	defer func() {
		if !handedOff {
			profile.finish(true)
		}
	}()
	phaseStart := coldProfileClock(profile)

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

	profile.duration("auth_seconds", phaseStart)
	phaseStart = coldProfileClock(profile)
	stem, err := backend.readHint(ctx, tok)
	if err != nil {
		return nil, err
	}
	profile.duration("hint_http_seconds", phaseStart)
	phaseStart = coldProfileClock(profile)
	metadataURL := fmt.Sprintf("%s/sage/obs/metadata/%s.metadata.json", backend.Warehouse, stem)
	arrowResult := arrowio.WantsResult(ctx)
	now := time.Now().UTC()
	var statement string
	if arrowResult {
		statement, err = buildColdArrowSQL(query, metadataURL, now)
	} else {
		statement, err = buildColdSQL(query, metadataURL, now)
	}
	if err != nil {
		return nil, err
	}
	profile.duration("sql_build_seconds", phaseStart)
	if arrowResult {
		result, err := backend.queryArrow(ctx, tok, statement)
		handedOff = err == nil
		return result, err
	}

	backend.mu.Lock()
	if backend.closed {
		backend.mu.Unlock()
		return nil, fmt.Errorf("cold backend is closed")
	}
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

// readHint reads the version-hint.text file.
func (backend *Backend) readHint(ctx context.Context, tok string) (string, error) {
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

// validHintStem checks if the stem is valid.
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

// coldResults is the results for the cold backend.
// Implements Results for a DuckDB query against cold storage.
type coldResults struct {
	rows    *sql.Rows
	conn    *sql.Conn
	release func()
	record  *model.Record
	err     error
}

// Err returns the error from the cold results.
func (r *coldResults) Err() error { return r.err }

// Close closes the cold results.
// Releases the database connection.
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
func (r *coldResults) Record() *model.Record { return r.record }

// Next advances the cold results to the next row.
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
