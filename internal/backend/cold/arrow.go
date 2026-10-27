// arrow.go contains the cold arrow state type and its associated functions.
package cold

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"

	"beehive-data-api/internal/arrowio"
	model "beehive-data-api/internal/query"
)

// coldArrowState is the arrow state for the cold backend.
// Accessed only while Backend.mu is held, including the reader's lifetime.
type coldArrowState struct {
	arrowDB *streamingArrowDB
}

// closeArrow closes the arrow database.
func (backend *Backend) closeArrow() error {
	if backend.arrowDB == nil {
		return nil
	}
	err := backend.arrowDB.Close()
	backend.arrowDB = nil
	return err
}

// queryArrow queries the cold backend.
// Runs statement through DuckDB's Arrow interface and returns the
// batches without materializing the complete result or scanning each row into
// a Go record. The caller builds statement with buildColdArrowSQL. The native
// database is initialized once per backend. Each request refreshes the scoped
// secret and owns a connection and result until Close. Keep the lock so
// cold queries share one resource budget.
func (backend *Backend) queryArrow(ctx context.Context, tok, statement string) (model.Results, error) {
	profile := coldProfileFromContext(ctx)
	phaseStart := coldProfileClock(profile)
	backend.mu.Lock()
	profile.duration("arrow_queue_wait_seconds", phaseStart)
	released := false
	release := func() {
		if !released {
			released = true
			backend.mu.Unlock()
		}
	}
	fail := func(err error) (model.Results, error) {
		release()
		return nil, err
	}
	if backend.closed {
		return fail(fmt.Errorf("cold backend is closed"))
	}
	if backend.connector == nil {
		return fail(fmt.Errorf("cold connector is nil"))
	}
	if strings.ContainsAny(tok, "'\\") {
		return fail(fmt.Errorf("refusing to interpolate a token that contains a quote"))
	}
	phaseStart = coldProfileClock(profile)
	if backend.arrowDB == nil {
		db, err := newStreamingArrowDB(ctx, coldDuckDBSetup(backend.IOSettings))
		if err != nil {
			return fail(err)
		}
		backend.arrowDB = db
	}
	profile.duration("arrow_engine_init_seconds", phaseStart)
	setup := []string{"SET TimeZone='UTC'", fmt.Sprintf(
		"CREATE OR REPLACE SECRET pelican (TYPE http, EXTRA_HTTP_HEADERS MAP {'Authorization': 'Bearer %s'})",
		tok)}
	rdr, err := backend.arrowDB.query(ctx, statement, setup)
	if err != nil {
		return fail(err)
	}
	if err := arrowio.SchemaCompatible(rdr.Schema()); err != nil {
		rdr.Release()
		return fail(err)
	}
	return &coldArrowResults{rdr: rdr, release: release, profile: profile}, nil
}

// coldArrowResults is the results for the cold arrow.
type coldArrowResults struct {
	rdr     array.RecordReader
	release func()
	err     error
	profile *coldQueryProfile
}

// Err returns the error from the cold arrow results.
func (r *coldArrowResults) Err() error { return r.err }

// Close closes the cold arrow results.
func (r *coldArrowResults) Close() error {
	if r.rdr != nil {
		r.err = r.rdr.Err()
		r.rdr.Release()
		r.rdr = nil
	}
	r.profile.finish(r.err != nil)
	r.release()
	return r.err
}

// Next is unused on this result.
// The arrow handler reads ArrowReader.
func (r *coldArrowResults) Next() bool { return false }

// Record returns the record from the cold arrow results.
func (r *coldArrowResults) Record() *model.Record { return nil }

// ArrowReader returns the arrow reader from the cold arrow results.
// The Arrow handler reads ArrowReader.
func (r *coldArrowResults) ArrowReader() array.RecordReader { return r.rdr }
