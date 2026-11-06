// aggregate_runtime_test.go runs the production HTTP and tiered paths on a local fixture.

package cold

import (
	"beehive-data-api/internal/backend/hot"
	"beehive-data-api/internal/backend/tiered"
	"beehive-data-api/internal/httpapi"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/csv"
	"encoding/json"
	"encoding/pem"
	"fmt"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This tests production routing/compiler, actual annotated CSV decoding, private
// hot Parquet staging, native execution and both encoders. No precompiled SQL
// adapter is used. All data are from a fresh local exp91 fixture, not production.
func TestAggregationProductionHTTP(t *testing.T) { runAggregationProductionHTTP(t, false, false) }

func TestAggregationLiveTierParity(t *testing.T) {
	if os.Getenv("AGGREGATION_PARITY_SEED_REPORT") == "" {
		t.Skip("set AGGREGATION_PARITY_SEED_REPORT and INFLUX_TOKEN for an isolated live Influx fixture")
	}
	runAggregationProductionHTTP(t, true, false)
}

func TestHeadTailProductionHTTP(t *testing.T) { runAggregationProductionHTTP(t, false, true) }

func TestHeadTailLiveTierParity(t *testing.T) {
	if os.Getenv("AGGREGATION_PARITY_SEED_REPORT") == "" {
		t.Skip("set AGGREGATION_PARITY_SEED_REPORT and INFLUX_TOKEN for the retained isolated live fixture")
	}
	runAggregationProductionHTTP(t, true, true)
}

func runAggregationProductionHTTP(t *testing.T, live, selection bool) {
	manifestPath := os.Getenv("COLD_AGGREGATION_MANIFEST")
	if manifestPath == "" {
		t.Skip("set COLD_AGGREGATION_MANIFEST to the local exp91 fixture")
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest aggregationFixtureManifest
	if err = json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	fixture := filepath.Clean(manifest.Root)
	if strings.Contains(filepath.ToSlash(fixture), "/node-data/") {
		t.Fatal("isolated fixture required")
	}
	var inputs []aggregationFixtureRecord
	raw, err = os.ReadFile(filepath.Join(fixture, "fixture.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &inputs); err != nil {
		t.Fatal(err)
	}
	// Serve immutable metadata under the production /sage/<scope>/warehouse
	// URL shape. Manifest/data references remain private local fixture files.
	raw, err = os.ReadFile(strings.TrimPrefix(manifest.Metadata, "file://"))
	if err != nil {
		t.Fatal(err)
	}
	metadataBytes := append([]byte(nil), raw...)
	warehousePath := "/sage/aggregation-parity/warehouse"
	metadataPath := warehousePath + "/sage/obs/metadata/" + filepath.Base(manifest.Metadata)
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != metadataPath {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		http.ServeContent(w, r, filepath.Base(metadataPath), time.Time{}, bytes.NewReader(metadataBytes))
	}))
	defer storage.Close()
	root := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(root, "key.pem")
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	backend, err := NewBackend(storage.URL+warehousePath, keyPath, defaultColdIOSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	backend.SourceBucket = "waggle"
	stageDir := t.TempDir()
	metadata := storage.URL + metadataPath
	backend.Client = storage.Client()
	var wantedName string
	hotCalls := 0
	client := influxdb2.NewClientWithOptions("https://hot.fixture", "fixture", influxdb2.DefaultOptions().SetHTTPClient(&http.Client{Transport: aggregationFixtureTransport(func(req *http.Request) (*http.Response, error) {
		hotCalls++
		if req.URL.Path != "/api/v2/query" {
			return nil, fmt.Errorf("unexpected hot operation")
		}
		var body struct {
			Query string `json:"query"`
		}
		if err = json.NewDecoder(req.Body).Decode(&body); err != nil {
			return nil, err
		}
		if !strings.Contains(body.Query, `r._measurement == "`+wantedName+`"`) {
			return nil, fmt.Errorf("raw hot filter missing")
		}
		for _, word := range []string{"aggregateWindow(", "mean()", "count()", "sum()", "min()", "max()", "limit(", "tail("} {
			if strings.Contains(body.Query, word) {
				return nil, fmt.Errorf("aggregation/limit pushed into raw hot query")
			}
		}
		bounds := regexp.MustCompile(`range\(start:([^,]+),stop:([^)]+)\)`).FindStringSubmatch(body.Query)
		if len(bounds) != 3 {
			return nil, fmt.Errorf("raw bounds missing: %s", body.Query)
		}
		low, e := time.Parse(time.RFC3339Nano, bounds[1])
		if e != nil {
			return nil, e
		}
		high, e := time.Parse(time.RFC3339Nano, bounds[2])
		if e != nil {
			return nil, e
		}
		var out bytes.Buffer
		writer := csv.NewWriter(&out)
		for index, row := range inputs {
			if row.Name != wantedName || row.Time < low.UnixNano() || row.Time >= high.UnixNano() {
				continue
			}
			keys := []string{}
			for k := range row.Meta {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			dtype := map[string]string{"int": "long", "uint": "unsignedLong", "float": "double", "bool": "boolean", "string": "string"}[row.Kind]
			if dtype == "" {
				return nil, fmt.Errorf("unexpected fixture kind")
			}
			types := []string{"#datatype", "string", "long", "dateTime:RFC3339", dtype, "string", "string"}
			header := []string{"", "result", "table", "_time", "_value", "_field", "_measurement"}
			text := string(row.Value)
			if row.Kind == "string" {
				if e = json.Unmarshal(row.Value, &text); e != nil {
					return nil, e
				}
			}
			record := []string{"", "_result", strconv.Itoa(index), time.Unix(0, row.Time).UTC().Format(time.RFC3339Nano), text, "value", row.Name}
			for _, k := range keys {
				types = append(types, "string")
				header = append(header, k)
				record = append(record, row.Meta[k])
			}
			writer.Write(types)
			writer.Write(header)
			writer.Write(record)
		}
		writer.Flush()
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/csv"}}, Body: io.NopCloser(bytes.NewReader(out.Bytes()))}, writer.Error()
	})}))
	org, bucket := "fixture", "waggle"
	if live {
		var seed struct {
			Completed        bool
			RawRowsVerified  int `json:"raw_rows_verified"`
			URL, Org, Bucket string
			ManifestSHA      string `json:"manifest_sha256"`
		}
		data, e := os.ReadFile(os.Getenv("AGGREGATION_PARITY_SEED_REPORT"))
		if e != nil {
			t.Fatal(e)
		}
		if e = json.Unmarshal(data, &seed); e != nil {
			t.Fatal(e)
		}
		manifestBytes, e := os.ReadFile(manifestPath)
		if e != nil {
			t.Fatal(e)
		}
		parsed, e := url.Parse(seed.URL)
		if !seed.Completed || seed.RawRowsVerified == 0 || seed.ManifestSHA != fmt.Sprintf("%x", sha256.Sum256(manifestBytes)) || !regexp.MustCompile(`^agg-exp86-[a-f0-9]{32}$`).MatchString(seed.Bucket) || e != nil || (parsed.Hostname() != "localhost" && parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "::1") || os.Getenv("INFLUX_TOKEN") == "" {
			t.Fatal("verified isolated local seed and token required")
		}
		client.Close()
		client = influxdb2.NewClientWithOptions(seed.URL, os.Getenv("INFLUX_TOKEN"), influxdb2.DefaultOptions().SetHTTPRequestTimeout(90))
		org, bucket = seed.Org, seed.Bucket
		backend.SourceBucket = bucket
	}
	defer client.Close()
	hotBackend := &hot.Backend{Client: client, Org: org, Bucket: bucket}
	cases := append([]aggregationFixtureCase(nil), manifest.Cases...)
	if live {
		for _, tc := range manifest.Cases {
			if strings.HasSuffix(tc.ID, "-cold_only") {
				tc.ID = strings.TrimSuffix(tc.ID, "-cold_only") + "-hot_only"
				tc.Border = tc.Request.Start
				cases = append(cases, tc)
			}
		}
	}
	if selection {
		cases = headTailFixtureCases(t, cases, inputs)
	}

	export := os.Getenv("AGGREGATION_HTTP_OUTPUT_DIR")
	if export != "" {
		export, err = filepath.Abs(export)
		if err != nil {
			t.Fatal(err)
		}
		for _, part := range strings.Split(filepath.ToSlash(export), "/") {
			if part == "node-data" {
				t.Fatal("isolated response output required")
			}
		}
		if err = os.Mkdir(export, 0700); err != nil {
			t.Fatal("new response output directory required", err)
		}
		spec := struct {
			Scope string                   `json:"scope"`
			Cases []aggregationFixtureCase `json:"cases"`
		}{"Actual production API routing/staging/encoders with isolated fixtures", cases}
		data, e := json.MarshalIndent(spec, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(export, "client-manifest.json"), data, 0600); e != nil {
			t.Fatal(e)
		}
	}
	for _, tc := range cases {
		for _, format := range []string{"ndjson", "arrow"} {
			t.Run(tc.ID+"/"+format, func(t *testing.T) {
				backend.ProfileDir = ""
				if os.Getenv("AGGREGATION_PARITY_PROFILE") == "1" {
					backend.ProfileDir = t.TempDir()
				}
				wantedName = tc.Request.Filter["name"]
				before := hotCalls
				border, err := time.Parse(time.RFC3339Nano, tc.Border)
				if err != nil {
					t.Fatal(err)
				}
				router := &tiered.Backend{Hot: hotBackend, Cold: backend, Border: border, TieredStagingDir: stageDir}
				releases := 0
				router.Coordinator = &tiered.Coordinator{URL: "https://coord.fixture", Token: "fixture", Bucket: bucket, Owner: "aggregation-test", Warehouse: backend.Warehouse,
					Client: &http.Client{Transport: aggregationFixtureTransport(func(req *http.Request) (*http.Response, error) {
						var request map[string]string
						if e := json.NewDecoder(req.Body).Decode(&request); e != nil {
							return nil, e
						}
						var payload any = map[string]bool{"ok": true}
						switch req.URL.Path {
						case "/v1/reads/acquire", "/v1/reads/renew":
							payload = tiered.View{ID: request["id"], Warehouse: backend.Warehouse, Metadata: metadata, Frontier: tc.Border, CoverageStart: "1678-01-01T00:00:00Z", Generation: 1, ExpiresAt: time.Now().Add(30 * time.Minute).Format(time.RFC3339Nano)}
						case "/v1/reads/release":
							if !backend.mu.TryLock() {
								return nil, fmt.Errorf("grant released before native result unlocked")
							}
							backend.mu.Unlock()
							entries, e := os.ReadDir(stageDir)
							if e != nil || len(entries) != 0 {
								return nil, fmt.Errorf("grant released before staging cleanup")
							}
							releases++
						default:
							return nil, fmt.Errorf("unexpected coordinator operation")
						}
						raw, e := json.Marshal(payload)
						return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(raw))}, e
					})}}
				service := httpapi.NewService(&httpapi.ServiceConfig{Backend: router})
				body, _ := json.Marshal(tc.Request)
				request := httptest.NewRequest(http.MethodPost, "/api/v1/query", bytes.NewReader(body))
				if format == "arrow" {
					request.Header.Set("Accept", "application/vnd.apache.arrow.stream")
				}
				response := httptest.NewRecorder()
				service.ServeHTTP(response, request)
				if response.Code != 200 {
					t.Fatalf("HTTP %d: %s", response.Code, response.Body.String())
				}
				aggregationFixtureCompare(t, aggregationFixtureDecode(t, format, response.Body.Bytes()), tc.Expected)
				if backend.ProfileDir != "" && format == "arrow" && !strings.HasSuffix(tc.ID, "-hot_only") {
					files, e := filepath.Glob(filepath.Join(backend.ProfileDir, "*.timing.json"))
					if e != nil || len(files) != 1 {
						t.Fatal("expected one shared staging/native profile", files, e)
					}
					data, e := os.ReadFile(files[0])
					var profile struct {
						Status string             `json:"status"`
						Phases map[string]float64 `json:"phases"`
					}
					if e != nil || json.Unmarshal(data, &profile) != nil || profile.Status != "complete" {
						t.Fatal("incomplete aggregate profile", string(data), e)
					}
					if strings.HasSuffix(tc.ID, "-split") {
						for _, key := range []string{"aggregate_hot_rows", "aggregate_input_bytes", "aggregate_parquet_bytes", "aggregate_hot_open_seconds", "aggregate_hot_read_stage_seconds", "aggregate_staging_complete_since_backend_start_seconds"} {
							if _, ok := profile.Phases[key]; !ok {
								t.Fatal("staging phase missing from shared profile", key)
							}
						}
					}
				}
				if export != "" {
					file, e := os.OpenFile(filepath.Join(export, tc.ID+"."+format), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
					if e != nil {
						t.Fatal(e)
					}
					_, e = file.Write(response.Body.Bytes())
					closed := file.Close()
					if e != nil || closed != nil {
						t.Fatal("response export failed", e, closed)
					}
				}
				if releases != 1 {
					t.Fatalf("grant releases %d != 1", releases)
				}
				wantCalls := 0
				if strings.HasSuffix(tc.ID, "-split") {
					wantCalls = 1
				}
				if !live && hotCalls-before != wantCalls {
					t.Fatalf("hot calls %d != %d", hotCalls-before, wantCalls)
				}
				entries, err := os.ReadDir(stageDir)
				if err != nil || len(entries) != 0 {
					t.Fatal("aggregation scratch leaked", entries, err)
				}
			})
		}
	}
}
