package main

import (
	"fmt"
	"net/http"
	"net/url"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/message-queue-go-producer/producer"
	"github.com/Financial-Times/service-status-go/gtg"
)

type HealthCheck struct {
	producer     producer.MessageProducer
	httpClient   *http.Client
	httpEndpoint string
}

func NewHealthCheck(p producer.MessageProducer, httpEndpoint string, client *http.Client) *HealthCheck {
	return &HealthCheck{
		producer:     p,
		httpClient:   client,
		httpEndpoint: httpEndpoint,
	}
}

func (h *HealthCheck) Health() func(w http.ResponseWriter, r *http.Request) {
	checks := make([]fthealth.Check, 0)
	if h.producer != nil {
		checks = append(checks, h.canConnectToProxy())
	} else {
		checks = append(checks, h.canConnectToHttpEndpoint())
	}
	hc := fthealth.HealthCheck{
		SystemCode:  "concept-publisher",
		Name:        "Concept Publisher",
		Description: "Checks if all the dependent services are reachable and healthy.",
		Checks:      checks,
	}
	return fthealth.Handler(hc)
}

func (h *HealthCheck) canConnectToProxy() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Forwarding messages to kafka-proxy in won't work. Concept publishing won't work.",
		Name:             "Forward messages to kafka-proxy.",
		PanicGuide:       "https://dewey.ft.com/concept-publisher.html",
		Severity:         1,
		TechnicalSummary: "Forwarding messages is broken. Check if kafka-proxy is reachable.",
		Checker:          h.producer.ConnectivityCheck,
	}
}

func (h *HealthCheck) canConnectToHttpEndpoint() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Forwarding messages to HTTP endpoint will fail. Concept publishing won't work.",
		Name:             "Forward messages to HTTP endpoint.",
		PanicGuide:       "https://dewey.ft.com/concept-publisher.html",
		Severity:         1,
		TechnicalSummary: "Forwarding messages is broken. Check if HTTP endpoint is reachable.",
		Checker:          h.canConnectToHttpEndpointCheck,
	}
}

func (h *HealthCheck) GTG() gtg.Status {
	if h.producer != nil {
		producerCheck := func() gtg.Status {
			return gtgCheck(h.producer.ConnectivityCheck)
		}
		return gtg.FailFastParallelCheck([]gtg.StatusChecker{producerCheck})()
	}
	httpCheck := func() gtg.Status {
		return gtgCheck(h.canConnectToHttpEndpointCheck)
	}
	return gtg.FailFastParallelCheck([]gtg.StatusChecker{httpCheck})()
}

func gtgCheck(handler func() (string, error)) gtg.Status {
	if _, err := handler(); err != nil {
		return gtg.Status{GoodToGo: false, Message: err.Error()}
	}
	return gtg.Status{GoodToGo: true}
}

func (h *HealthCheck) canConnectToHttpEndpointCheck() (string, error) {
	url, err := url.Parse(h.httpEndpoint)
	url.Path = "/__gtg"
	if err != nil {
		return "", err
	}
	req := &http.Request{
		Method: "GET",
		URL:    url,
		Header: http.Header{
			"Content-Type": {"application/json"},
		},
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("Could not connect to HTTP endpoint: %v", err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP endpoint returned status: %d", resp.StatusCode)
	}
	return "Connectivity to HTTP endpoint is OK", nil
}
