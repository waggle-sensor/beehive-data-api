// record_test.go contains the tests for the cold record functions.
package cold

import (
	"database/sql"
	"encoding/json"
	"testing"

	model "beehive-data-api/internal/query"
)

// TestColdVariantJSON tests the cold variant JSON.
func TestColdVariantJSON(t *testing.T) {
	for _, value := range []string{"9007199254740993", "9223372036854775807", "-9223372036854775808", "1.25", `"123"`, "true", "null", `"中文\n"`} {
		raw, err := coldValueJSON(sql.NullString{String: value, Valid: true})
		if err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(model.Record{Value: raw})
		if err != nil {
			t.Fatal(err)
		}
		var record map[string]json.RawMessage
		if err := json.Unmarshal(body, &record); err != nil {
			t.Fatal(err)
		}
		if string(record["value"]) != value {
			t.Fatalf("value changed: %s", body)
		}
	}
	for _, value := range []string{"NaN", "Infinity", "invalid", ""} {
		if _, err := coldValueJSON(sql.NullString{String: value, Valid: true}); err == nil {
			t.Fatalf("accepted invalid JSON %q", value)
		}
	}
	raw, err := coldValueJSON(sql.NullString{})
	if err != nil || string(raw) != "null" {
		t.Fatalf("null: %s %v", raw, err)
	}
}
