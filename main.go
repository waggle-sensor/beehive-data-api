package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	addr := flag.String("addr", ":10000", "service addr")
	requestQueueSize := flag.Int("request-queue-size", 10, "service request queue size")
	requestQueueTimeout := flag.Duration("request-queue-timeout", 10*time.Second, "service request queue timeout duration")
	influxdbURL := flag.String("influxdb.url", getenv("INFLUXDB_URL", "http://localhost:8086"), "influxdb url")
	influxdbToken := flag.String("influxdb.token", getenv("INFLUXDB_TOKEN", ""), "influxdb token")
	influxdbBucket := flag.String("influxdb.bucket", getenv("INFLUXDB_BUCKET", ""), "influxdb bucket")
	influxdbTimeout := flag.Duration("influxdb.timeout", mustParseDuration(getenv("INFLUXDB_TIMEOUT", "15m")), "influxdb client timeout")
	rabbitmqURL := flag.String("rabbitmq.url", getenv("RABBITMQ_URL", ""), "rabbitmq url")
	flag.Parse()

	log.Printf("connecting to influxdb at %s", *influxdbURL)
	client := influxdb2.NewClient(*influxdbURL, *influxdbToken)
	defer client.Close()

	// TODO figure out reasonable timeout on potentially large result sets
	client.Options().HTTPClient().Timeout = *influxdbTimeout

	// NOTE temporarily redirecting to sage docs. can change to something better later.
	http.Handle("/", http.RedirectHandler("https://docs.waggle-edge.ai/docs/tutorials/accessing-data", http.StatusTemporaryRedirect))

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/whoami", whoamiHandler)

	svc := NewService(&ServiceConfig{
		Backend: &InfluxBackend{
			Client: client,
			Org:    "waggle",
			Bucket: *influxdbBucket,
		},
		RequestQueueSize:    requestQueueSize,
		RequestQueueTimeout: requestQueueTimeout,
	})
	http.Handle("/api/v1/query", svc)

	streamSvc := &StreamService{
		RabbitMQURL: *rabbitmqURL,
	}
	// TODO fix this when verifying internal cacert
	streamSvc.TLSConfig = &tls.Config{}
	streamSvc.TLSConfig.InsecureSkipVerify = true
	http.Handle("/api/v0/stream", streamSvc)

	log.Printf("service listening on %s", *addr)
	log.Printf("request queue size is %d with %s timeout", *requestQueueSize, *requestQueueTimeout)
	if err := http.ListenAndServe(*addr, nil); err != nil {
		log.Fatal(err)
	}
}

func whoamiHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "hello and thanks for visiting!\n\nhere's some info about your request!\n\n")

	fmt.Fprintf(w, "headers:\n")
	for k, v := range r.Header {
		fmt.Fprintf(w, "%q: %q\n", k, v)
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
