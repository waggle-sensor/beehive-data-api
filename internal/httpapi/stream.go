// stream.go serves live measurements as server-sent events. Each connection
// has its own exclusive queue and acknowledges messages as it receives them.

package httpapi

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	streamConnectionsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Name:      "stream_connections_total",
		Help:      "The total number of open stream connections.",
	})
)

// extractFilterTopicsFromName uses the name filter as RabbitMQ binding keys
// and removes it, so the remaining entries are metadata matchers.
func extractFilterTopicsFromName(filter map[string]string) []string {
	if name, ok := filter["name"]; ok {
		delete(filter, "name")
		return strings.Split(name, "|")
	}
	return []string{"#"}
}

// buildMatchers compiles each remaining filter value as a regular expression
// matched against that metadata key.
func buildMatchers(filter map[string]string) (map[string]*regexp.Regexp, error) {
	matchers := map[string]*regexp.Regexp{}

	for key, pattern := range filter {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
		matchers[key] = re
	}

	return matchers, nil
}

// Message is one live measurement written to the client.
type Message struct {
	Name      string            `json:"name"`
	Timestamp time.Time         `json:"timestamp"`
	Value     interface{}       `json:"value"`
	Meta      map[string]string `json:"meta"`
}

// unmarshalMessage reads the broker payload. Its timestamp is nanoseconds in "ts"
// and its measurement value is "val".
func unmarshalMessage(b []byte, msg *Message) error {
	var obj struct {
		Name      string            `json:"name"`
		Timestamp int64             `json:"ts"`
		Value     interface{}       `json:"val"`
		Meta      map[string]string `json:"meta"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return err
	}

	msg.Name = obj.Name
	msg.Timestamp = time.Unix(0, obj.Timestamp).UTC()
	msg.Value = obj.Value
	msg.Meta = obj.Meta

	return nil
}

// matchMessage reports whether every matcher matches the corresponding metadata value.
// A missing key is an empty string and usually fails the match.
func matchMessage(matchers map[string]*regexp.Regexp, msg *Message) bool {
	for key, matcher := range matchers {
		if !matcher.MatchString(msg.Meta[key]) {
			return false
		}
	}
	return true
}

// getFilterForQueryValues keeps the first value of each query key.
func getFilterForQueryValues(values url.Values) map[string]string {
	filter := make(map[string]string)
	for key := range values {
		filter[key] = values.Get(key)
	}
	return filter
}

// StreamService opens a RabbitMQ connection per HTTP request.
type StreamService struct {
	RabbitMQURL       string
	HeartbeatDuration time.Duration
	metrics           *apiMetrics
}

// ServeHTTP opens one exclusive queue, binds the requested names, and writes
// matching messages as SSE. A disconnect after the stream starts is a canceled
// session. A setup failure replaces that outcome.
func (svc *StreamService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	metrics := svc.metrics
	if metrics == nil {
		metrics = defaultAPIMetrics
	}
	observation := &streamObservation{metrics: metrics, started: time.Now(), outcome: "unsupported"}
	defer func() {
		recovered := recover()
		if recovered != nil {
			observation.fail("panic", "panic")
		}
		observation.finish()
		if recovered != nil {
			panic(recovered)
		}
	}()
	_, ok := w.(http.Flusher)
	if !ok {
		metrics.streamErrors.WithLabelValues("request").Inc()
		return
	}

	if r.Method != http.MethodGet {
		observation.fail("request", "invalid_request")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	observation.outcome = "canceled"
	streamConnectionsTotal.Add(1)
	metrics.streamStarted.Inc()
	defer streamConnectionsTotal.Add(-1)

	// TODO need to bound URL size here
	filter := getFilterForQueryValues(r.URL.Query())

	// special case: vsn is always uppercase
	if value, ok := filter["vsn"]; ok {
		filter["vsn"] = strings.ToUpper(value)
	}

	// special case: node is always lowercase
	if value, ok := filter["node"]; ok {
		filter["node"] = strings.ToLower(value)
	}

	// extract topics from name filter. deletes name field afterwards.
	topics := extractFilterTopicsFromName(filter)

	// create matcher
	matchers, err := buildMatchers(filter)
	if err != nil {
		observation.fail("filter", "invalid_request")
		log.Printf("invalid request filter: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	conn, err := amqp.Dial(svc.RabbitMQURL)
	if err != nil {
		observation.fail("dial", "broker_error")
		log.Printf("failed dial rabbitmq: %s", err)
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		return
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		observation.fail("channel", "broker_error")
		log.Printf("failed to open rabbitmq channel: %s", err)
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		return
	}
	defer channel.Close()

	queue, err := channel.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		observation.fail("queue", "broker_error")
		log.Printf("failed to declare queue: %s", err)
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		return
	}

	for _, topic := range topics {
		if err := channel.QueueBind(queue.Name, topic, "waggle.msg", false, nil); err != nil {
			observation.fail("bind", "broker_error")
			log.Printf("failed to bind queue %s to exchange", queue.Name)
			http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
			return
		}
	}

	messages, err := channel.Consume(queue.Name, "", true, false, false, false, nil)
	if err != nil {
		observation.fail("consume", "broker_error")
		log.Printf("failed to consume queue %s", queue.Name)
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		return
	}

	svc.streamEvents(w, r, messages, matchers, observation)
}

// streamEvents handles an established broker subscription without changing its acknowledgements.
func (svc *StreamService) streamEvents(w http.ResponseWriter, r *http.Request, messages <-chan amqp.Delivery, matchers map[string]*regexp.Regexp, observation *streamObservation) {
	metrics := observation.metrics
	flush := func() bool {
		if err := http.NewResponseController(w).Flush(); err != nil {
			observation.fail("write", "write_error")
			return false
		}
		return true
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ticker := time.NewTicker(svc.HeartbeatDuration)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			n, err := fmt.Fprintf(w, ":keepalive\n\n")
			metrics.streamBytes.Add(float64(n))
			if err != nil {
				observation.fail("write", "write_error")
				return
			}
			if !flush() {
				return
			}
			metrics.streamHeartbeats.Inc()
		case amqpMsg, open := <-messages:
			if !open {
				observation.fail("broker_close", "broker_error")
				return
			}
			// reset heartbeat ticker
			ticker.Reset(svc.HeartbeatDuration)

			var msg Message

			if err := unmarshalMessage(amqpMsg.Body, &msg); err != nil {
				metrics.streamMessages.WithLabelValues("invalid").Inc()
				metrics.streamErrors.WithLabelValues("decode").Inc()
				continue
			}

			if !matchMessage(matchers, &msg) {
				metrics.streamMessages.WithLabelValues("filtered").Inc()
				continue
			}

			b, err := json.Marshal(msg)
			if err != nil {
				metrics.streamMessages.WithLabelValues("invalid").Inc()
				observation.fail("encode", "write_error")
				return
			}

			// write and flush event to client
			n, err := fmt.Fprintf(w, "event: message\ndata: %s\n\n", b)
			metrics.streamBytes.Add(float64(n))
			if err != nil {
				observation.fail("write", "write_error")
				return
			}
			if !flush() {
				return
			}
			metrics.streamMessages.WithLabelValues("written").Inc()
		}
	}
}
