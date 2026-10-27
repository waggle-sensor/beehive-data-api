// profile_test.go contains the tests for the cold query profile type.
package cold

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestColdQueryProfileDisabled tests the cold query profile disabled.
func TestColdQueryProfileDisabled(t *testing.T) {
	dir, err := NewProfileSession("")
	if err != nil || dir != "" {
		t.Fatalf("disabled session: %q %v", dir, err)
	}
	p, err := newColdQueryProfile(context.Background(), "")
	if err != nil || p != nil {
		t.Fatalf("disabled profile: %v %v", p, err)
	}
	p.duration("test", coldProfileClock(p))
	p.mark("test")
	p.batch(1)
	p.endOfStream()
	p.finish(false)
	if len(p.setupSQL()) != 0 {
		t.Fatal("disabled profiling generated SQL")
	}
}

// TestColdQueryProfileReports tests the cold query profile reports.
func TestColdQueryProfileReports(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "operator's profiles")
	dir, err := NewProfileSession(parent)
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewProfileSession(parent)
	if err != nil || dir == other {
		t.Fatal("sessions must be isolated", err)
	}
	for _, path := range []string{parent, dir} {
		st, err := os.Stat(path)
		if err != nil || st.Mode().Perm() != 0700 {
			t.Fatalf("private directory %s: %v", path, err)
		}
	}
	for _, status := range []string{"complete", "failed", "cancelled", "incomplete"} {
		t.Run(status, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			p, err := newColdQueryProfile(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			if coldProfileFromContext(context.WithValue(ctx, coldQueryProfileKey{}, p)) != p {
				t.Fatal("profile missing from context")
			}
			sql := p.setupSQL()
			if len(sql) != 2 || sql[0] != "SET enable_profiling='json'" || !strings.Contains(sql[1], "operator''s profiles") {
				t.Fatalf("unsafe profiler setup: %v", sql)
			}
			p.duration("test_seconds", coldProfileClock(p))
			p.batch(3)
			p.batch(4)
			if status == "complete" {
				p.endOfStream()
				if err := os.WriteFile(p.file, []byte(`{"children":[]}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if status == "cancelled" {
				cancel()
			}
			p.finish(status == "failed")
			path := strings.TrimSuffix(p.file, ".duckdb.json") + ".timing.json"
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			p.finish(true)
			again, _ := os.ReadFile(path)
			if string(raw) != string(again) {
				t.Fatal("finish rewrote final status")
			}
			var report struct {
				Status        string
				Rows, Batches int64
				DuckDBFile    string `json:"duckdb_file"`
				Present       bool   `json:"duckdb_profile_present"`
				Phases        map[string]float64
			}
			if err := json.Unmarshal(raw, &report); err != nil {
				t.Fatal(err)
			}
			if report.Status != status || report.Rows != 7 || report.Batches != 2 || report.Present != (status == "complete") {
				t.Fatalf("bad report: %s", raw)
			}
			if report.DuckDBFile != filepath.Base(p.file) {
				t.Fatal("profile path must be relative")
			}
			for _, phase := range []string{"first_batch_since_backend_start_seconds", "total_backend_lifetime_seconds", "test_seconds"} {
				if _, ok := report.Phases[phase]; !ok {
					t.Fatal("missing phase", phase)
				}
			}
			for _, f := range []string{p.file, path} {
				st, err := os.Stat(f)
				if err != nil || st.Mode().Perm() != 0600 {
					t.Fatalf("private file %s: %v", f, err)
				}
			}
		})
	}
}
