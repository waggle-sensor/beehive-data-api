// arrow_stream.go contains the code for the streaming arrow reader for the cold backend.
package cold

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	bindings "github.com/duckdb/duckdb-go-bindings"
	"github.com/duckdb/duckdb-go/v2/arrowmapping"
	"github.com/duckdb/duckdb-go/v2/mapping"
)

// streamingArrowReader is the streaming arrow reader for the cold backend type.
// Uses DuckDB's streaming execution API. The pinned Go
// driver's Arrow.QueryContext calls the same non-streaming PendingPrepared as
// database/sql: fetching one batch at a time does not prevent that API from
// retaining the entire query result first, outside memory_limit.
//
// The driver does not expose its native connection or a streaming query method.
// Use a backend-owned native database instead of accessing private driver fields
// or changing process-global mapping functions. Native results, the connection,
// and the interrupt watcher remain query-scoped; extensions and caches can be
// reused by subsequent connections.
type streamingArrowReader struct {
	ctx       context.Context
	conn      mapping.Connection
	res       mapping.Result
	opts      arrowmapping.ArrowOptions
	schema    *arrow.Schema
	current   arrow.RecordBatch
	err       error
	refs      int
	mu        sync.Mutex
	stop      chan struct{}
	stopped   chan struct{}
	tempDir   string
	onRelease func()
	profile   *coldQueryProfile
}

// streamingArrowDB is the streaming arrow database for the cold backend type.
// A backend serializes query/Close calls with its existing mutex. Each reader
// must be released before the database is closed. temp_directory is database-
// scoped: retain one directory until Close instead of resetting it mid-query.
type streamingArrowDB struct {
	db      mapping.Database
	tempDir string
}

// newStreamingArrowDB creates a new streaming arrow database for the cold backend.
func newStreamingArrowDB(ctx context.Context, setup []string) (_ *streamingArrowDB, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db := &streamingArrowDB{}
	db.tempDir, err = os.MkdirTemp("", "beehive-cold-arrow-")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			db.Close()
		}
	}()
	if mapping.Open("", &db.db) == mapping.StateError {
		return nil, fmt.Errorf("open streaming DuckDB instance")
	}
	r, err := db.newReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Release()
	if err := r.execSetup(append([]string{"SET temp_directory=" + sqlQuote(db.tempDir)}, setup...)); err != nil {
		return nil, err
	}
	return db, nil
}

// Close closes the streaming arrow database for the cold backend.
func (db *streamingArrowDB) Close() error {
	if db.db.Ptr != nil {
		mapping.Close(&db.db)
	}
	if db.tempDir != "" {
		err := os.RemoveAll(db.tempDir)
		db.tempDir = ""
		return err
	}
	return nil
}

var _ array.RecordReader = (*streamingArrowReader)(nil)

// newStreamingArrowReader creates a new streaming arrow reader.
// Standalone reader for local experiments. Production reuses streamingArrowDB.
func newStreamingArrowReader(ctx context.Context, statement string, setup []string) (*streamingArrowReader, error) {
	db, err := newStreamingArrowDB(ctx, nil)
	if err != nil {
		return nil, err
	}
	r, err := db.query(ctx, statement, setup)
	if err != nil {
		db.Close()
		return nil, err
	}
	r.onRelease = func() { db.Close() }
	return r, nil
}

// newReader creates a new streaming arrow reader.
func (db *streamingArrowDB) newReader(ctx context.Context) (*streamingArrowReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if db.db.Ptr == nil {
		return nil, fmt.Errorf("streaming DuckDB instance is closed")
	}
	r := &streamingArrowReader{ctx: ctx, refs: 1, tempDir: db.tempDir}
	if mapping.Connect(db.db, &r.conn) == mapping.StateError {
		r.Release()
		return nil, fmt.Errorf("connect streaming DuckDB instance")
	}
	r.stop, r.stopped = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(r.stopped)
		select {
		case <-ctx.Done():
			mapping.Interrupt(r.conn)
		case <-r.stop:
		}
	}()
	return r, nil
}

// execSetup executes the setup for the streaming arrow reader.
func (r *streamingArrowReader) execSetup(setup []string) error {
	for _, q := range setup {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		var result mapping.Result
		state := mapping.Query(r.conn, q, &result)
		message := mapping.ResultError(&result)
		mapping.DestroyResult(&result)
		if state == mapping.StateError {
			// Setup may contain a bearer token. Never include SQL in errors.
			return errors.Join(r.ctx.Err(), fmt.Errorf("streaming DuckDB setup: %s", message))
		}
	}
	return r.ctx.Err()
}

// query queries the streaming arrow database for the cold backend.
func (db *streamingArrowDB) query(ctx context.Context, statement string, setup []string) (_ *streamingArrowReader, err error) {
	r, err := db.newReader(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			r.Release()
		}
	}()
	r.profile = coldProfileFromContext(ctx)
	if r.profile != nil {
		r.profile.engineVersion = mapping.LibraryVersion()
	}
	phaseStart := coldProfileClock(r.profile)
	if err := r.execSetup(setup); err != nil {
		return nil, err
	}
	r.profile.duration("connection_setup_seconds", phaseStart)
	phaseStart = coldProfileClock(r.profile)
	if err := r.execSetup(r.profile.setupSQL()); err != nil {
		return nil, err
	}
	r.profile.duration("profiling_setup_seconds", phaseStart)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	phaseStart = coldProfileClock(r.profile)
	var stmt mapping.PreparedStatement
	state := mapping.Prepare(r.conn, statement, &stmt)
	defer mapping.DestroyPrepare(&stmt)
	if state == mapping.StateError {
		return nil, errors.Join(ctx.Err(), fmt.Errorf("prepare streaming query: %s", mapping.PrepareError(stmt)))
	}
	r.profile.duration("prepare_seconds", phaseStart)
	phaseStart = coldProfileClock(r.profile)
	var pending mapping.PendingResult
	// This C API is deprecated but still documented in DuckDB 2.0;
	// unlike PendingPrepared it permits streaming the SELECT's result chunks.
	state = bindings.PendingPreparedStreaming(stmt, &pending)
	defer mapping.DestroyPending(&pending)
	if state == mapping.StateError {
		return nil, errors.Join(ctx.Err(), fmt.Errorf("start streaming query: %s", mapping.PendingError(pending)))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.profile.duration("pending_seconds", phaseStart)
	phaseStart = coldProfileClock(r.profile)
	if mapping.ExecutePending(pending, &r.res) == mapping.StateError {
		return nil, errors.Join(ctx.Err(), fmt.Errorf("execute streaming query: %s", mapping.ResultError(&r.res)))
	}
	r.profile.duration("execute_pending_seconds", phaseStart)
	phaseStart = coldProfileClock(r.profile)
	r.opts = arrowmapping.ResultGetArrowOptions(&r.res)
	count := mapping.ColumnCount(&r.res)
	names, types := make([]string, count), make([]mapping.LogicalType, count)
	for i := range count {
		names[i] = mapping.ColumnName(&r.res, i)
		types[i] = mapping.ColumnLogicalType(&r.res, i)
	}
	defer func() {
		for i := range count {
			mapping.DestroyLogicalType(&types[i])
		}
	}()
	schema, ed := arrowmapping.NewArrowSchema(r.opts, types, names)
	defer mapping.DestroyErrorData(&ed)
	if mapping.ErrorDataHasError(ed) {
		return nil, fmt.Errorf("streaming Arrow schema: %s", mapping.ErrorDataMessage(ed))
	}
	r.schema = schema
	r.profile.duration("arrow_schema_seconds", phaseStart)
	r.profile.mark("native_ready_since_backend_start_seconds")
	return r, nil
}

// Schema returns the schema for the streaming arrow reader.
func (r *streamingArrowReader) Schema() *arrow.Schema { return r.schema }

// Retain retains the streaming arrow reader.
func (r *streamingArrowReader) Retain() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refs > 0 {
		r.refs++
	}
}

// Release releases the streaming arrow reader.
func (r *streamingArrowReader) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refs == 0 {
		return
	}
	r.refs--
	if r.refs > 0 {
		return
	}
	if r.stop != nil {
		close(r.stop)
		<-r.stopped // Never interrupt a disconnected connection.
	}
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.opts.Ptr != nil {
		arrowmapping.DestroyArrowOptions(&r.opts)
	}
	mapping.DestroyResult(&r.res)
	if r.conn.Ptr != nil {
		mapping.Disconnect(&r.conn)
	}
	if r.onRelease != nil {
		r.onRelease()
		r.onRelease = nil
	}
}

// Next advances the streaming arrow reader.
func (r *streamingArrowReader) Next() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refs == 0 || r.err != nil {
		return false
	}
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.err = r.ctx.Err(); r.err != nil {
		return false
	}
	phaseStart := coldProfileClock(r.profile)
	chunk := mapping.FetchChunk(r.res)
	r.profile.duration("fetch_chunks_seconds", phaseStart)
	if chunk.Ptr == nil {
		r.err = r.ctx.Err()
		if message := mapping.ResultError(&r.res); message != "" {
			r.err = errors.Join(r.err, errors.New(message))
		}
		if r.err == nil {
			r.profile.endOfStream()
		}
		return false
	}
	defer mapping.DestroyDataChunk(&chunk)
	phaseStart = coldProfileClock(r.profile)
	rec, ed := arrowmapping.DataChunkToArrowArray(r.opts, r.schema, chunk)
	defer mapping.DestroyErrorData(&ed)
	if mapping.ErrorDataHasError(ed) {
		r.err = fmt.Errorf("streaming Arrow batch: %s", mapping.ErrorDataMessage(ed))
		return false
	}
	r.current = rec
	r.profile.duration("arrow_conversion_seconds", phaseStart)
	r.profile.batch(rec.NumRows())
	return true
}

// Record returns the record for the streaming arrow reader.
func (r *streamingArrowReader) Record() arrow.RecordBatch { return r.RecordBatch() }

// RecordBatch returns the record batch for the streaming arrow reader.
func (r *streamingArrowReader) RecordBatch() arrow.RecordBatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current
}

// Err returns the error for the streaming arrow reader.
func (r *streamingArrowReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}
