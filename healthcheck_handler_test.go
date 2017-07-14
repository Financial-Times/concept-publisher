package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Financial-Times/message-queue-go-producer/producer"
	"github.com/stretchr/testify/assert"
)

func initializeHealthCheckWithProducer(isProducerConnectionHealthy bool) *HealthCheck {
	return &HealthCheck{
		producer:   &mockProducerInstance{isConnectionHealthy: isProducerConnectionHealthy},
		httpClient: http.DefaultClient,
	}
}

func initializeHealthCheckWithoutProducer(isHttpConnectionHealthy bool) (*httptest.Server, *HealthCheck) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	mux.HandleFunc("/__gtg", func(w http.ResponseWriter, r *http.Request) {
		if isHttpConnectionHealthy {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	hc := &HealthCheck{
		producer:     nil,
		httpEndpoint: server.URL,
		httpClient:   http.DefaultClient,
	}

	return server, hc
}

func TestNewHealthCheckWithProducer(t *testing.T) {
	hc := NewHealthCheck(producer.NewMessageProducer(producer.MessageProducerConfig{}), "endpoint", http.DefaultClient)

	assert.NotNil(t, hc.producer)
	assert.NotNil(t, hc.httpClient)
	assert.Equal(t, "endpoint", hc.httpEndpoint)
}

func TestNewHealthCheckWithoutProducer(t *testing.T) {
	hc := NewHealthCheck(nil, "endpoint", http.DefaultClient)

	assert.Nil(t, hc.producer)
	assert.NotNil(t, hc.httpClient)
	assert.Equal(t, "endpoint", hc.httpEndpoint)
}

func TestHappyHealthCheckWithProducer(t *testing.T) {
	hc := initializeHealthCheckWithProducer(true)

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Forward messages to kafka-proxy.","ok":true`, "Forward messages to kafka-proxy healthcheck should be happy")
}

func TestHappyHealthCheckWithoutProducer(t *testing.T) {
	server, hc := initializeHealthCheckWithoutProducer(true)
	defer server.Close()

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Forward messages to HTTP endpoint.","ok":true`, "Forward messages to HTTP endpoint healthcheck should be happy")
}

func TestHealthCheckWithUnhappyProducer(t *testing.T) {
	hc := initializeHealthCheckWithProducer(false)

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Forward messages to kafka-proxy.","ok":false`, "Forward messages to kafka-proxy healthcheck should be happy")
}

func TestHealthCheckWithUnhappyHttpEndpoint(t *testing.T) {
	server, hc := initializeHealthCheckWithoutProducer(false)
	defer server.Close()

	req := httptest.NewRequest("GET", "http://example.com/__health", nil)
	w := httptest.NewRecorder()

	hc.Health()(w, req)

	assert.Equal(t, 200, w.Code, "It should return HTTP 200 OK")
	assert.Contains(t, w.Body.String(), `"name":"Forward messages to HTTP endpoint.","ok":false`, "Forward messages to HTTP endpoint healthcheck should be unhappy")
}

func TestGTGHappyFlowWithProducer(t *testing.T) {
	hc := initializeHealthCheckWithProducer(true)

	status := hc.GTG()
	assert.True(t, status.GoodToGo)
	assert.Empty(t, status.Message)
}

func TestGTGHappyFlowWithoutProducer(t *testing.T) {
	server, hc := initializeHealthCheckWithoutProducer(true)
	defer server.Close()

	status := hc.GTG()
	assert.True(t, status.GoodToGo)
	assert.Empty(t, status.Message)
}

func TestGTGUnhappyFlowWithProducer(t *testing.T) {
	hc := initializeHealthCheckWithProducer(false)

	status := hc.GTG()
	assert.False(t, status.GoodToGo)
	assert.Equal(t, "Error connecting to the queue", status.Message)
}

func TestGTGUnhappyFlowWithoutProducer(t *testing.T) {
	server, hc := initializeHealthCheckWithoutProducer(false)
	defer server.Close()

	status := hc.GTG()
	assert.False(t, status.GoodToGo)
	assert.Equal(t, "HTTP endpoint returned status: 503", status.Message)
}

type mockProducerInstance struct {
	isConnectionHealthy bool
}

func (p *mockProducerInstance) SendMessage(string, producer.Message) error {
	return nil
}

func (p *mockProducerInstance) ConnectivityCheck() (string, error) {
	if p.isConnectionHealthy {
		return "", nil
	}

	return "", errors.New("Error connecting to the queue")
}
