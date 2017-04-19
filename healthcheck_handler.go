package main

import (
	"fmt"
	"net/http"

	"net/url"

	fthealth "github.com/Financial-Times/go-fthealth"
	log "github.com/Sirupsen/logrus"
)

type healthcheckHandler struct {
	kafkaPAddr   string
	topic        string
	httpClient   *http.Client
	httpEndpoint string
}

func newHealthcheckHandler(topic string, kafkaPAddr string, httpClient *http.Client, httpEndpoint string) healthcheckHandler {
	return healthcheckHandler{
		kafkaPAddr:   kafkaPAddr,
		topic:        topic,
		httpClient:   httpClient,
		httpEndpoint: httpEndpoint,
	}
}

func (h *healthcheckHandler) health() func(w http.ResponseWriter, r *http.Request) {

	if h.kafkaPAddr != "" {
		return fthealth.Handler("Dependent services healthcheck", "Services: kafka-rest-proxy", h.canConnectToProxyHealthcheck())
	}
	return fthealth.Handler("Dependent services healthcheck", "Services: http-endpoint", h.canConnectToHttpEndpoint())

}

func (h *healthcheckHandler) gtg(w http.ResponseWriter, r *http.Request) {

	if h.kafkaPAddr != "" {
		if err := h.checkCanConnectToProxy(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	} else {
		if err := h.checkCanConnectToHttpEndpoint(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}
}

func (h *healthcheckHandler) canConnectToProxyHealthcheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Forwarding messages to kafka-proxy in coco won't work. Concept publishing won't work.",
		Name:             "Forward messages to kafka-proxy.",
		PanicGuide:       "https://dewey.ft.com/concept-publisher.html",
		Severity:         1,
		TechnicalSummary: "Forwarding messages is broken. Check if kafka-proxy in coco is reachable.",
		Checker:          h.checkCanConnectToProxy,
	}
}

func (h *healthcheckHandler) canConnectToHttpEndpoint() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Forwarding messages to HTTP endpoint will fail. Concept publishing won't work.",
		Name:             "Forward messages to HTTP endpoint.",
		PanicGuide:       "https://dewey.ft.com/concept-publisher.html",
		Severity:         1,
		TechnicalSummary: "Forwarding messages is broken. Check if HTTP endpoint is reachable.",
		Checker:          h.checkCanConnectToHttpEndpoint,
	}
}

func (h *healthcheckHandler) checkCanConnectToHttpEndpoint() error {
	url, err := url.Parse(h.httpEndpoint)
	url.Path = "/__gtg"
	if err != nil {
		return err
	}
	req := &http.Request{
		Method: "GET",
		URL:    url,
		Header: http.Header{
			"Content-Type": {"application/json"},
		},
	}

	resp, err := h.httpClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return err
	}
	return nil
}

func (h *healthcheckHandler) checkCanConnectToProxy() error {
	err := h.checkProxyConnection()
	if err != nil {
		log.Errorf("Healthcheck: Error reading request body: %v", err.Error())
		return err
	}
	return nil
}

func (h *healthcheckHandler) checkProxyConnection() error {
	//check if proxy is running
	req, err := http.NewRequest("GET", h.kafkaPAddr+"/topics", nil)
	if err != nil {
		log.Errorf("Error creating new kafka-proxy healthcheck request: %v", err.Error())
		return err
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		log.Errorf("Healthcheck: Error executing kafka-proxy GET request: %v", err.Error())
		return err
	}
	defer closeNice(resp)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Connecting to kafka proxy was not successful. Status: %d", resp.StatusCode)
	}
	return nil
}
