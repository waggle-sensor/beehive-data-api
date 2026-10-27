// settings_test.go contains the tests for the cold settings type.
package cold

import (
	"strings"
	"testing"
)

// TestColdIOSettingsValidation tests the cold I/O settings validation.
func TestColdIOSettingsValidation(t *testing.T) {
	for _, pair := range [][2]string{{"0", "-1"}, {"257", "-1"}, {"-1", "-1"}, {"8;SELECT 1", "-1"}, {"8", "1"}, {"8", "-2"}, {"8", "bad"}} {
		if _, err := ParseIOSettings(pair[0], pair[1]); err == nil {
			t.Fatalf("accepted invalid settings %v", pair)
		}
	}
	for _, depth := range []string{"-1", "0"} {
		config, err := ParseIOSettings("8", depth)
		if err != nil {
			t.Fatal(err)
		}
		sql := strings.Join(coldDuckDBSetup(&config), "; ")
		if !strings.Contains(sql, "SET async_threads=8") || !strings.Contains(sql, "SET read_ahead_depth="+depth) {
			t.Fatal(sql)
		}
	}
}
