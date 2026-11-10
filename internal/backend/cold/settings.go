// settings.go fixes the cold DuckDB session. Two CPU threads and 3GB are not
// startup flags. Async threads and read-ahead are the only adjustable I/O settings.

package cold

import (
	"fmt"
	"strconv"
)

// Bound remote I/O separately from the two CPU workers. Automatic read-ahead
// (-1) negotiates memory with the sort; positive fixed depths bypass that budget.
type IOSettings struct {
	AsyncThreads   int
	ReadAheadDepth int
}

// defaultColdIOSettings is eight async threads and memory-budgeted read-ahead.
func defaultColdIOSettings() IOSettings {
	return IOSettings{AsyncThreads: 8, ReadAheadDepth: -1}
}

// ParseIOSettings accepts 1..256 async threads. Read-ahead is -1 for the
// memory-budgeted default or 0 to disable lookahead.
func ParseIOSettings(async, ahead string) (IOSettings, error) {
	threads, err := strconv.Atoi(async)
	if err != nil || threads < 1 || threads > 256 {
		return IOSettings{}, fmt.Errorf("cold.async-threads must be between 1 and 256")
	}
	depth, err := strconv.Atoi(ahead)
	if err != nil || depth < -1 || depth > 0 {
		return IOSettings{}, fmt.Errorf("cold.read-ahead-depth must be -1 (memory-budgeted) or 0 (no lookahead)")
	}
	return IOSettings{threads, depth}, nil
}

// coldDuckDBRuntimeSetup is the per-connection session: UTC, two threads,
// a 3GB limit, Arrow IPC 1.0, and the configured I/O settings.
func coldDuckDBRuntimeSetup(settings *IOSettings) []string {
	config := defaultColdIOSettings()
	if settings != nil {
		config = *settings
	}
	return []string{
		"SET TimeZone='UTC'", "SET threads=2", "SET memory_limit='3GB'",
		"SET arrow_output_version='1.0'",
		fmt.Sprintf("SET async_threads=%d", config.AsyncThreads),
		fmt.Sprintf("SET read_ahead_depth=%d", config.ReadAheadDepth),
	}
}

// coldDuckDBSetup is the runtime session plus the httpfs and iceberg extensions.
func coldDuckDBSetup(settings *IOSettings) []string {
	return append(coldDuckDBRuntimeSetup(settings), "INSTALL httpfs", "LOAD httpfs", "INSTALL iceberg", "LOAD iceberg")
}
