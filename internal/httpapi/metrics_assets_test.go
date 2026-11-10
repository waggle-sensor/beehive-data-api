// metrics_assets_test.go checks that the dashboard and alerts name only series
// this process exports, and that every panel query is scoped to its selected target.

package httpapi

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// TestDashboardUsesExportedMetricsAndScopedTargets tests the dashboard uses exported metrics and scoped targets.
func TestDashboardUsesExportedMetricsAndScopedTargets(t *testing.T) {
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{}
	for _, family := range families {
		name := family.GetName()
		known[name] = true
		switch family.GetType().String() {
		case "HISTOGRAM":
			known[name+"_bucket"] = true
			fallthrough
		case "SUMMARY":
			known[name+"_sum"] = true
			known[name+"_count"] = true
		}
	}
	path := filepath.Join("..", "..", "observability", "grafana", "beehive-data-api.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var dashboard struct {
		UID     string `json:"uid"`
		Refresh string `json:"refresh"`
		Schema  int    `json:"schemaVersion"`
		Panels  []struct {
			ID      int    `json:"id"`
			Type    string `json:"type"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err = json.Unmarshal(data, &dashboard); err != nil {
		t.Fatal(err)
	}
	if dashboard.UID != "beehive-data-api" || dashboard.Schema != 38 || dashboard.Refresh != "30s" {
		t.Fatal("unexpected dashboard identity/schema")
	}
	names := regexp.MustCompile(`(?:dataapi|hot_to_cold)_[a-z_]+`)
	check := func(expr string) {
		for _, name := range names.FindAllString(expr, -1) {
			if !known[name] {
				t.Error("dashboard/alert references unexported metric", name)
			}
		}
	}
	ids := map[int]bool{}
	for _, panel := range dashboard.Panels {
		if ids[panel.ID] {
			t.Fatal("duplicate panel ID", panel.ID)
		}
		ids[panel.ID] = true
		if panel.Type == "row" {
			continue
		}
		if len(panel.Targets) == 0 {
			t.Fatal("missing panel query")
		}
		for _, target := range panel.Targets {
			check(target.Expr)
			if !strings.Contains(target.Expr, `job=~"$job"`) || !strings.Contains(target.Expr, `instance=~"$instance"`) {
				t.Fatal("unscoped query", target.Expr)
			}
		}
	}
	alerts, err := os.ReadFile(filepath.Join("..", "..", "observability", "prometheus", "alerts.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	check(string(alerts))
}
