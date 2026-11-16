package query

import "testing"

func TestTableIdentity(t *testing.T) {
	var defaults TableIdentity
	if defaults.Namespace() != "sage" || defaults.Name() != "observations" || defaults.Path() != "sage/observations" {
		t.Fatal(defaults)
	}
	custom, err := NewTableIdentity("telemetry", "readings_2026")
	if err != nil {
		t.Fatal(err)
	}
	if custom.Location("https://origin/sage/cold/lakehouse") != "https://origin/sage/cold/lakehouse/telemetry/readings_2026" {
		t.Fatal(custom)
	}
	for _, value := range []string{"", ".", "..", "a/b", "a.b", "a%2fb", "a b", "a-b", "1table", "name?x", "name#x"} {
		if _, err := NewTableIdentity(value, "observations"); err == nil {
			t.Fatal("invalid namespace", value)
		}
		if _, err := NewTableIdentity("sage", value); err == nil {
			t.Fatal("invalid table", value)
		}
	}
}
