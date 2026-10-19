package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	addr := flag.String("addr", ":10000", "service addr")
	influxdbURL := flag.String("influxdb.url", getenv("INFLUXDB_URL", "http://localhost:8086"), "influxdb url")
	influxdbToken := flag.String("influxdb.token", getenv("INFLUXDB_TOKEN", ""), "influxdb token")
	influxdbBucket := flag.String("influxdb.bucket", getenv("INFLUXDB_BUCKET", ""), "influxdb bucket")
	influxdbTimeout := flag.Duration("influxdb.timeout", mustParseDuration(getenv("INFLUXDB_TIMEOUT", "15m")), "influxdb client timeout")
	coldWarehouse := flag.String("cold.warehouse", getenv("COLD_WAREHOUSE", ""), "Iceberg warehouse URL on Pelican; enables the tiered backend when set")
	coldWatermark := flag.String("cold.watermark", getenv("COLD_WATERMARK", ""), "RFC3339 instant; data before it is served from cold, at or after from hot")
	pelicanKey := flag.String("pelican.key", getenv("PELICAN_KEY_PATH", ""), "PEM key that signs SciTokens for the cold warehouse")
	rabbitmqURL := flag.String("rabbitmq.url", getenv("RABBITMQ_URL", ""), "rabbitmq url")
	streamHeartbeatDuration := flag.Duration("stream.heartbeat-duration", mustParseDuration(getenv("STREAM_HEARTBEAT_DURATION", "15s")), "stream heartbeat duration")
	flag.Parse()

	log.Printf("connecting to influxdb at %s", *influxdbURL)
	client := influxdb2.NewClient(*influxdbURL, *influxdbToken)
	defer client.Close()

	// TODO figure out reasonable timeout on potentially large result sets
	client.Options().HTTPClient().Timeout = *influxdbTimeout

	var backend Backend = &InfluxBackend{
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
		cold, err := buildColdBackend(*coldWarehouse, *pelicanKey)
		if err != nil {
			log.Fatalf("cold backend: %s", err)
		}
		defer cold.DB.Close()
		log.Printf("tiered backend: duckdb iceberg_scan on %s, watermark %s", *coldWarehouse, watermark.Format(time.RFC3339Nano))
		backend = &TieredBackend{
			Hot:       backend,
			Cold:      cold,
			Watermark: watermark,
		}
	}

	querySvc := NewService(&ServiceConfig{Backend: backend})

	streamSvc := &StreamService{
		RabbitMQURL:       *rabbitmqURL,
		HeartbeatDuration: *streamHeartbeatDuration,
	}

	// NOTE temporarily redirecting to sage docs. can change to something better later.
	http.Handle("/", http.RedirectHandler("https://docs.waggle-edge.ai/docs/tutorials/accessing-data", http.StatusTemporaryRedirect))
	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/api/v1/query", querySvc)
	http.Handle("/api/v0/stream", streamSvc)

	log.Printf("service listening on %s", *addr)
	if err := http.ListenAndServe(*addr, nil); err != nil {
		log.Fatal(err)
	}
}

func getenv(key string, fallback string) string {
	if s, ok := os.LookupEnv(key); ok {
		return s
	}
	return fallback
}

func mustParseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return d
}
