package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	log "github.com/Sirupsen/logrus"
	"net/url"
	"github.com/gorilla/mux"
)

type publishHandler struct {
	publishService *publishService
	//health  healthcheckHandler
}

type createJobRequest struct {
	Concept       string   `json:"concept"`
	URL           string   `json:"url"`
	Throttle      int      `json:"throttle"`
	Authorization string   `json:"authorization"`
	IDS           []string `json:"ids"`
}

func newPublishHandler(publishService *publishService) publishHandler {
	return publishHandler{publishService: publishService}
}

func (j createJobRequest) String() string {
	return fmt.Sprintf("Concept=%s URL=%s Throttle=%d", j.Concept, j.URL, j.Throttle)
}

func (h *publishHandler) createJob(w http.ResponseWriter, r *http.Request) {
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
	theJob, err := h.publishService.newJob(jr.Concept, jr.IDS, u, jr.Authorization, jr.Throttle)
	go h.publishService.runJob(theJob, jr.Authorization)
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(theJob.JobID); err != nil {
		log.Errorf("Error on json encoding=%v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *publishHandler) status(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	status, err := h.publishService.getJobStatus(id)
	if err != nil {
		log.Errorf("Error returning job. %v\n", err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(status); err != nil {
		log.Errorf("Error on json encoding=%v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *publishHandler) listJobs(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(h.publishService.getJobIds()); err != nil {
		log.Errorf("Error on json encoding=%v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
