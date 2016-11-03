package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	fthealth "github.com/Financial-Times/go-fthealth"
	log "github.com/Sirupsen/logrus"
	"errors"
)

type healthcheck struct {
	kafkaPAddr string
	topic      string
	httpClient *http.Client
}

func newHealthcheck(topic string, kafkaPAddr string, httpClient *http.Client) healthcheck {
	return healthcheck{
		kafkaPAddr: kafkaPAddr,
		topic:      topic,
		httpClient: httpClient,
	}
}

func (h *healthcheck) health() func(w http.ResponseWriter, r *http.Request) {
	return fthealth.Handler("Dependent services healthcheck", "Services: kafka-rest-proxy", h.canConnectToProxyHealthcheck())
}

func (h *healthcheck) gtg(w http.ResponseWriter, r *http.Request) {
	if err := h.checkCanConnectToProxy(); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

func (h *healthcheck) canConnectToProxyHealthcheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Forwarding messages to kafka-proxy in coco won't work. Concept publishing won't work.",
		Name:             "Forward messages to kafka-proxy.",
		PanicGuide:       "https://sites.google.com/a/ft.com/ft-technology-service-transition/home/run-book-library/concept-publisher",
		Severity:         1,
		TechnicalSummary: "Forwarding messages is broken. Check if kafka-proxy in coco is reachable.",
		Checker:          h.checkCanConnectToProxy,
	}
}

func (h *healthcheck) checkCanConnectToProxy() error {
	body, err := checkProxyConnection(h.kafkaPAddr)
	if err != nil {
		log.Errorf("Healthcheck: Error reading request body: %v", err.Error())
		return err
	}
	return checkIfTopicIsPresent(body, h.topic)
}

func checkProxyConnection(address string) (body []byte, err error) {
	//check if proxy is running and topic is present
	req, err := http.NewRequest("GET", address+"/topics", nil)
	if err != nil {
		log.Errorf("Error creating new kafka-proxy healthcheck request: %v", err.Error())
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Errorf("Healthcheck: Error executing kafka-proxy GET request: %v", err.Error())
		return nil, err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Connecting to kafka proxy was not successful. Status: %d", resp.StatusCode)
	}
	return ioutil.ReadAll(resp.Body)
}

func checkIfTopicIsPresent(body []byte, searchedTopic string) error {
	var topics []string
	err := json.Unmarshal(body, &topics)
	if err != nil {
		return fmt.Errorf("Connection could be established to kafka-proxy, but a parsing error occured and topic could not be found. %v", err.Error())
	}
	for _, topic := range topics {
		if topic == searchedTopic {
			return nil
		}
	}
	return errors.New("Connection could be established to kafka-proxy, but topic was not found")
}

