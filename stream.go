package main

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

func getFilterTopics(filter map[string]string) []string {
	if name, ok := filter["name"]; ok {
		return strings.Split(name, "|")
	}
	return []string{"#"}
}

func buildMatchers(filter map[string]string) (map[string]*regexp.Regexp, error) {
	matchers := map[string]*regexp.Regexp{}

	for k, v := range filter {
		re, err := regexp.Compile(v)
		if err != nil {
			return nil, err
		}
		matchers[k] = re
	}

	return matchers, nil
}

type Message struct {
	Name      string            `json:"name"`
	Timestamp time.Time         `json:"timestamp"`
	Value     interface{}       `json:"value"`
	Meta      map[string]string `json:"meta"`
}

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

func matchMessage(matchers map[string]*regexp.Regexp, msg *Message) bool {
	for key, matcher := range matchers {
		if !matcher.MatchString(msg.Meta[key]) {
			return false
		}
	}
	return true
}

func getFilterForQueryValues(values url.Values) map[string]string {
	filter := make(map[string]string)
	for k := range values {
		filter[k] = values.Get(k)
	}
	return filter
}

type StreamService struct {
	RabbitMQURL string
}

func (svc *StreamService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	streamConnectionsTotal.Add(1)
	defer streamConnectionsTotal.Add(-1)

	// TODO need to bound URL size here
	filter := getFilterForQueryValues(r.URL.Query())

	// get topics and ensure name field is deleted before constructing other field matchers
	topics := getFilterTopics(filter)
	delete(filter, "name")

	// create matcher
	matchers, err := buildMatchers(filter)
	if err != nil {
		log.Printf("invalid request filter: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	conn, err := amqp.Dial(svc.RabbitMQURL)
	if err != nil {
		log.Printf("failed dial rabbitmq: %s", err)
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("failed to open rabbitmq channel: %s", err)
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		return
	}
	defer ch.Close()

	queue, err := ch.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		log.Printf("failed to declare queue: %s", err)
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		return
	}

	for _, topic := range topics {
		if err := ch.QueueBind(queue.Name, topic, "waggle.msg", false, nil); err != nil {
			log.Printf("failed to bind queue %s to exchange", queue.Name)
			http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
			return
		}
	}

	messages, err := ch.Consume(queue.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Printf("failed to consume queue %s", queue.Name)
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	for {
		select {
		case <-r.Context().Done():
			return
		case amqpMsg := <-messages:
			var msg Message

			if err := unmarshalMessage(amqpMsg.Body, &msg); err != nil {
				continue
			}

			if !matchMessage(matchers, &msg) {
				continue
			}

			b, err := json.Marshal(msg)
			if err != nil {
				return
			}

			// write and flush event to client
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", b)
			flusher.Flush()
		}
	}
}
