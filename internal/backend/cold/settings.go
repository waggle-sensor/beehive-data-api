// settings.go contains the cold settings type and its associated functions.
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

// defaultColdIOSettings returns the default cold I/O settings.
func defaultColdIOSettings() IOSettings {
	return IOSettings{AsyncThreads: 8, ReadAheadDepth: -1}
}

// ParseIOSettings parses the cold I/O settings.
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

// coldDuckDBRuntimeSetup returns the runtime setup for the DuckDB.
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

// coldDuckDBSetup returns the setup for the DuckDB.
func coldDuckDBSetup(settings *IOSettings) []string {
	return append(coldDuckDBRuntimeSetup(settings), "INSTALL httpfs", "LOAD httpfs", "INSTALL iceberg", "LOAD iceberg")
}
