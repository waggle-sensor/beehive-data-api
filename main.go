package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"beehive-data-api/internal/arrowio"
	"beehive-data-api/internal/backend/cold"
	"beehive-data-api/internal/backend/hot"
	"beehive-data-api/internal/backend/tiered"
	"beehive-data-api/internal/httpapi"
	model "beehive-data-api/internal/query"
)

func main() {
	addr := flag.String("addr", ":10000", "service addr")
	arrowCompressionFlag := flag.String("arrow.compression", getenv("ARROW_COMPRESSION", "zstd"), "Arrow IPC compression: none, zstd, or lz4")
	influxdbURL := flag.String("influxdb.url", getenv("INFLUXDB_URL", "http://localhost:8086"), "influxdb url")
	influxdbToken := flag.String("influxdb.token", getenv("INFLUXDB_TOKEN", ""), "influxdb token")
	influxdbBucket := flag.String("influxdb.bucket", getenv("INFLUXDB_BUCKET", ""), "influxdb bucket")
	influxdbTimeout := flag.Duration("influxdb.timeout", mustParseDuration(getenv("INFLUXDB_TIMEOUT", "15m")), "influxdb client timeout")
	coldWarehouse := flag.String("cold.warehouse", getenv("COLD_WAREHOUSE", ""), "Iceberg warehouse URL on Pelican; enables the tiered backend when set")
	coldAsyncThreads := flag.String("cold.async-threads", getenv("COLD_ASYNC_THREADS", "8"), "cold DuckDB asynchronous I/O threads (1..256)")
	coldReadAhead := flag.String("cold.read-ahead-depth", getenv("COLD_READ_AHEAD_DEPTH", "-1"), "cold DuckDB lookahead: -1 for memory-budgeted automatic, 0 for none")
	coldProfileDir := flag.String("cold.profile-dir", getenv("COLD_PROFILE_DIR", ""), "optional local parent directory for cold Arrow operator profiles and timings")
	coldWatermark := flag.String("cold.watermark", getenv("COLD_WATERMARK", ""), "RFC3339 instant; data before it is served from cold, at or after from hot")
	pelicanKey := flag.String("pelican.key", getenv("PELICAN_KEY_PATH", ""), "PEM key that signs SciTokens for the cold warehouse")
	rabbitmqURL := flag.String("rabbitmq.url", getenv("RABBITMQ_URL", ""), "rabbitmq url")
	streamHeartbeatDuration := flag.Duration("stream.heartbeat-duration", mustParseDuration(getenv("STREAM_HEARTBEAT_DURATION", "15s")), "stream heartbeat duration")
	flag.Parse()
	compression, err := arrowio.ParseCompression(*arrowCompressionFlag)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("connecting to influxdb at %s", *influxdbURL)
	client := influxdb2.NewClient(*influxdbURL, *influxdbToken)
	defer client.Close()

	// TODO figure out reasonable timeout on potentially large result sets
	client.Options().HTTPClient().Timeout = *influxdbTimeout

	var backend model.Backend = &hot.Backend{
		Client: client,
		Org:    "waggle",
		Bucket: *influxdbBucket,
	}

	// The tiered backend is opt-in. With COLD_WAREHOUSE unset the service
	// behaves exactly as it always has, so the single-tier path stays the
	// default and its tests keep covering what ships by default.
	if *coldWarehouse != "" {
		if *coldWatermark == "" {
			log.Fatal("cold.warehouse requires cold.watermark")
		}
		if *pelicanKey == "" {
			log.Fatal("cold.warehouse requires pelican.key")
		}
		watermark, err := time.Parse(time.RFC3339, *coldWatermark)
		if err != nil {
			log.Fatalf("invalid cold.watermark %q: %s", *coldWatermark, err)
		}
		ioSettings, err := cold.ParseIOSettings(*coldAsyncThreads, *coldReadAhead)
		if err != nil {
			log.Fatal(err)
		}
		coldBackend, err := cold.NewBackend(*coldWarehouse, *pelicanKey, ioSettings)
		if err != nil {
			log.Fatalf("cold backend: %s", err)
		}
		defer coldBackend.Close()
		coldBackend.ProfileDir, err = cold.NewProfileSession(*coldProfileDir)
		if err != nil {
			log.Fatalf("cold profile directory: %s", err)
		}
		if coldBackend.ProfileDir != "" {
			log.Printf("cold query profiling enabled in %s", coldBackend.ProfileDir)
		}
		log.Printf("tiered backend: duckdb iceberg_scan on %s, watermark %s", *coldWarehouse, watermark.Format(time.RFC3339Nano))
		backend = &tiered.Backend{
			Hot:       backend,
			Cold:      coldBackend,
			Watermark: watermark,
		}
	}

	// create query service
	querySvc := httpapi.NewService(&httpapi.ServiceConfig{Backend: backend, ArrowCompression: compression})

	// create stream service
	streamSvc := &httpapi.StreamService{
		RabbitMQURL:       *rabbitmqURL,
		HeartbeatDuration: *streamHeartbeatDuration,
	}

	// handle http requests
	http.Handle("/", http.RedirectHandler("https://sagecontinuum.org/docs/tutorials/accessing-data", http.StatusTemporaryRedirect))
	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/api/v1/query", querySvc)
	http.Handle("/api/v0/stream", streamSvc)

	// start http server
	log.Printf("service listening on %s", *addr)
	if err := http.ListenAndServe(*addr, nil); err != nil {
		log.Fatal(err)
	}
}

// getenv is a helper function to get an environment variable with a fallback
func getenv(key string, fallback string) string {
	if s, ok := os.LookupEnv(key); ok {
		return s
	}
	return fallback
}

// mustParseDuration is a helper function to parse a duration with a fallback
func mustParseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return d
}
