package main

import (
	_ "net/http/pprof"
	"os"

	"net/http"

	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"

	fthealth "github.com/Financial-Times/go-fthealth"
	"github.com/Financial-Times/message-queue-go-producer/producer"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
)

func main() {
	app := cli.App("concept-publisher", "Retrieves concepts and puts them on a queue")
	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "PORT",
	})
	proxyAddr := app.String(cli.StringOpt{
		Name:   "proxy-address",
		Value:  "http://localhost:8080",
		Desc:   "Address used by the producer to connect to the queue",
		EnvVar: "PROXY_ADDR",
	})
	topic := app.String(cli.StringOpt{
		Name:   "destination-topic",
		Value:  "NativeCmsMetadataPublicationEvents",
		Desc:   "The topic to write the V1 metadata to",
		EnvVar: "TOPIC",
	})
	transAddr := app.String(cli.StringOpt{
		Name:   "transformer-addr",
		Value:  "http://localhost:8080",
		Desc:   "Address used by the producer to connect to the queue",
		EnvVar: "TRANSFORMER_ADDR",
	})
	app.Action = func() {
		messageProducer := producer.NewMessageProducer(producer.MessageProducerConfig{Addr: *proxyAddr, Topic: *topic})
		addr, err := url.Parse(*transAddr)
		if err != nil {
			log.Fatalf("Invalid transformer URL: %v (%v)", *transAddr, err)
		}
		s := newPublishService(addr, messageProducer)
		h := handler{service: &s, health: health{addr: *proxyAddr, topic: *topic}}
		serve(*port, h)
	}
	app.Run(os.Args)
}

func serve(port string, h handler) {

	m := mux.NewRouter()
	http.Handle("/", handlers.CombinedLoggingHandler(os.Stdout, m))
	m.HandleFunc("/jobs", h.createJob).Methods("POST")
	m.HandleFunc("/jobs", h.listJobs).Methods("GET")
	m.HandleFunc("/jobs/{id}", h.status).Methods("GET")
	m.HandleFunc("/__health", fthealth.Handler("Dependent services healthcheck", "Services: kafka-rest-proxy", h.health.canConnectToProxyHealthcheck()))
	m.HandleFunc("/__gtg", h.gtg)

	log.Infof("Listening on [%v].\n", port)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Printf("Web server failed: [%v].\n", err)
	}
}

type createJobRequest struct {
	Concept       string   `json:"concept"`
	URL           string   `json:"url"`
	Throttle      int      `json:"throttle"`
	Authorization string   `json:"authorization"`
	IDS           []string `json:"ids"`
}

func (j createJobRequest) String() string {
	return fmt.Sprintf("Concept=%s URL=%s Throttle=%d", j.Concept, j.URL, j.Throttle)
}

type job struct {
	JobID string `json:"jobId"`
}

type jobStatus struct {
	Concept  string   `json:"concept"`
	IDS      []string `json:"ids,omitempty"`
	URL      string   `json:"url"`
	Throttle int      `json:"throttle"`
	Count    int      `json:"count"`
	Done     int      `json:"done"`
	Status   string   `json:"status"`
}

func (j jobStatus) String() string {
	return fmt.Sprintf("Concept=%s URL=%s Count=%d Throttle=%d Status=%s", j.Concept, j.URL, j.Count, j.Throttle, j.Status)
}

type handler struct {
	service pubService
	health  health
}

func (h *handler) createJob(w http.ResponseWriter, r *http.Request) {
	var jr createJobRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&jr); err != nil {
		err := fmt.Sprintf("Invalid payload: (%v)", err)
		log.Errorf(err)
		http.Error(w, err, http.StatusBadRequest)
		return
	}
	log.Infof("ConceptPublish: Request received %v", jr)
	if jr.Concept == "" {
		err := "Concept empty"
		log.Errorf(err)
		http.Error(w, err, http.StatusBadRequest)
		return
	}
	if jr.URL == "" {
		err := "Base url empty"
		log.Errorf(err)
		http.Error(w, err, http.StatusBadRequest)
		return
	}
	u, err := url.Parse(jr.URL)
	if err != nil {
		log.Errorf("Invalid url: %v (%v)", jr.URL, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if jr.Throttle < 1 {
		err := fmt.Sprintf("Invalid throttle: %v", jr.Throttle)
		log.Errorf(err)
		http.Error(w, err, http.StatusBadRequest)
		return
	}

	id := h.service.newJob(jr.Concept, jr.IDS, u, jr.Authorization, jr.Throttle)
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(job{JobID: id}); err != nil {
		log.Errorf("Error on json encoding=%v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *handler) status(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	status, _ := h.service.jobStatus(id)
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(status); err != nil {
		log.Errorf("Error on json encoding=%v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *handler) listJobs(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(h.service.getJobs()); err != nil {
		log.Errorf("Error on json encoding=%v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *handler) gtg(w http.ResponseWriter, r *http.Request) {
	if err := h.health.checkCanConnectToProxy(); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

type health struct {
	addr  string
	topic string
}

func (h *health) canConnectToProxyHealthcheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Forwarding messages to kafka-proxy in coco won't work. Concept publishing won't work.",
		Name:             "Forward messages to kafka-proxy.",
		PanicGuide:       "https://sites.google.com/a/ft.com/ft-technology-service-transition/home/run-book-library/concept-publisher",
		Severity:         1,
		TechnicalSummary: "Forwarding messages is broken. Check if kafka-proxy in coco is reachable.",
		Checker:          h.checkCanConnectToProxy,
	}
}

func (h *health) checkCanConnectToProxy() error {
	body, err := checkProxyConnection(h.addr)
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

	return fmt.Errorf("Connection could be established to kafka-proxy, but topic was not found")
}
