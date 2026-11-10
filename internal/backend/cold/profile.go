// profile.go records a cold query plan when profiling is enabled. An empty
// parent directory leaves profiling off.

package cold

import (
	"beehive-data-api/internal/arrowio"
	model "beehive-data-api/internal/query"
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// NewProfileSession creates a new cold query profile session.
// Operator-only local diagnostics. No request flags or response/schema changes.
// Each API startup gets a private session directory; each query gets unique files.
func NewProfileSession(parent string) (string, error) {
	if parent == "" {
		return "", nil
	}
	root, err := filepath.Abs(parent)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return "", err
	}
	return os.MkdirTemp(root, "session-")
}

// coldQueryProfileKey is the key for the cold query profile.
type coldQueryProfileKey struct{}

// coldQueryProfile is the cold query profile.
type coldQueryProfile struct {
	mu            sync.Mutex
	once          sync.Once
	ctx           context.Context
	start         time.Time
	file          string
	phases        map[string]float64
	rows          int64
	batches       int64
	eof           bool
	first         bool
	engineVersion string
}

// newColdQueryProfile creates a new cold query profile.
func newColdQueryProfile(ctx context.Context, dir string) (*coldQueryProfile, error) {
	if dir == "" {
		return nil, nil
	}
	f, err := os.CreateTemp(dir, "query-*.duckdb.json")
	if err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	return &coldQueryProfile{ctx: ctx, start: time.Now(), file: f.Name(), phases: make(map[string]float64)}, nil
}

// coldProfileFromContext gets the cold query profile from the context.
func coldProfileFromContext(ctx context.Context) *coldQueryProfile {
	p, _ := ctx.Value(coldQueryProfileKey{}).(*coldQueryProfile)
	return p
}

// coldProfileClock gets the clock for the cold query profile.
func coldProfileClock(p *coldQueryProfile) time.Time {
	if p == nil {
		return time.Time{}
	}
	return time.Now()
}

// value records a diagnostic count or size, separately from elapsed phases.
func (p *coldQueryProfile) value(name string, value float64) {
	if p != nil {
		p.mu.Lock()
		p.phases[name] = value
		p.mu.Unlock()
	}
}

// duration adds the duration of a phase to the cold query profile.
func (p *coldQueryProfile) duration(phase string, start time.Time) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.phases[phase] += time.Since(start).Seconds()
}

// mark marks a phase in the cold query profile.
func (p *coldQueryProfile) mark(phase string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.phases[phase] = time.Since(p.start).Seconds()
}

// batch adds a batch to the cold query profile.
func (p *coldQueryProfile) batch(rows int64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rows += rows
	p.batches++
	if !p.first && rows > 0 {
		p.phases["first_batch_since_backend_start_seconds"] = time.Since(p.start).Seconds()
		p.first = true
	}
}

// endOfStream marks the end of the stream for the cold query profile.
func (p *coldQueryProfile) endOfStream() {
	if p != nil {
		p.mu.Lock()
		p.eof = true
		p.phases["native_eof_since_backend_start_seconds"] = time.Since(p.start).Seconds()
		p.mu.Unlock()
	}
}

// setupSQL sets up the SQL for the cold query profile.
func (p *coldQueryProfile) setupSQL() []string {
	if p == nil {
		return nil
	}
	// Call only after secrets have been installed. Profiling is connection-local.
	// DuckDB validates the output extension against the active profiler format.
	// Select JSON before assigning the .json filename.
	return []string{"SET enable_profiling='json'", "SET profiling_output=" + sqlQuote(p.file)}
}

// finish finishes the cold query profile.
func (p *coldQueryProfile) finish(failed bool) {
	if p == nil {
		return
	}
	p.once.Do(func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		status := "incomplete"
		switch {
		case failed:
			status = "failed"
		case p.eof:
			// A finished stream stays complete when the request context ends
			// after EOF, which is how a successful Arrow response closes.
			status = "complete"
		case p.ctx.Err() != nil:
			status = "cancelled"
		}
		p.phases["total_backend_lifetime_seconds"] = time.Since(p.start).Seconds()
		stat, err := os.Stat(p.file)
		present := err == nil && stat.Size() > 0
		if present {
			if err := os.Chmod(p.file, 0600); err != nil {
				log.Printf("cold profile permissions: %v", err)
			}
		}
		report := struct {
			EngineVersion string             `json:"engine_version,omitempty"`
			Version       int                `json:"version"`
			Status        string             `json:"status"`
			Rows          int64              `json:"rows"`
			Batches       int64              `json:"batches"`
			DuckDBFile    string             `json:"duckdb_file"`
			DuckDBPresent bool               `json:"duckdb_profile_present"`
			Phases        map[string]float64 `json:"phases"`
		}{p.engineVersion, 1, status, p.rows, p.batches, filepath.Base(p.file), present, p.phases}
		raw, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			log.Printf("cold profile summary: %v", err)
			return
		}
		path := strings.TrimSuffix(p.file, ".duckdb.json") + ".timing.json"
		if err := os.WriteFile(path, append(raw, '\n'), 0600); err != nil {
			log.Printf("cold profile write: %v", err)
			return
		}
		log.Printf("cold profile saved status=%s rows=%d timing=%s", status, p.rows, path)
	})
}

// BeginQueryProfile starts the native lifetime before tiered input staging.
func (backend *Backend) BeginQueryProfile(ctx context.Context) (context.Context, model.QueryProfile) {
	profile := coldProfileFromContext(ctx)
	if profile == nil && arrowio.WantsResult(ctx) && backend.ProfileDir != "" {
		var err error
		profile, err = newColdQueryProfile(ctx, backend.ProfileDir)
		if err != nil {
			log.Printf("query profile creation: %v", err)
		} else {
			ctx = context.WithValue(ctx, coldQueryProfileKey{}, profile)
		}
	}
	if profile == nil {
		return ctx, model.NoQueryProfile{}
	}
	observer := queryProfile{profile}
	return model.WithQueryProfile(ctx, observer), observer
}

// queryProfile adapts the cold profile to the interface shared with hot staging.
type queryProfile struct{ cold *coldQueryProfile }

func (adapter queryProfile) Enabled() bool { return adapter.cold != nil }
func (adapter queryProfile) Clock() time.Time {
	return coldProfileClock(adapter.cold)
}
func (adapter queryProfile) Duration(name string, start time.Time) {
	adapter.cold.duration(name, start)
}
func (adapter queryProfile) Value(name string, value float64) { adapter.cold.value(name, value) }
func (adapter queryProfile) Mark(name string)                 { adapter.cold.mark(name) }
func (adapter queryProfile) Finish(failed bool)               { adapter.cold.finish(failed) }
