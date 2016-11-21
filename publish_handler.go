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
	return fmt.Sprintf("conceptType=%s URL=\"%s\" Throttle=%d", j.Concept, j.URL, j.Throttle)
}

func (h *publishHandler) createJob(w http.ResponseWriter, r *http.Request) {
	var jobRequest createJobRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&jobRequest); err != nil {
		err := fmt.Sprintf("Invalid payload: (%v)", err)
		log.Errorf(err)
		http.Error(w, err, http.StatusBadRequest)
		return
	}
	log.Infof("message=\"Concept publish request received\" %v", jobRequest)
	if jobRequest.Concept == "" {
		err := "Concept empty"
		log.Errorf(err)
		http.Error(w, err, http.StatusBadRequest)
		return
	}
	if jobRequest.URL == "" {
		err := "Base url empty"
		log.Errorf(err)
		http.Error(w, err, http.StatusBadRequest)
		return
	}
	url, err := url.Parse(jobRequest.URL)
	if err != nil {
		log.Errorf("Invalid url: %v (%v)", jobRequest.URL, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if jobRequest.Throttle < 1 {
		err := fmt.Sprintf("Invalid throttle: %v", jobRequest.Throttle)
		log.Errorf(err)
		http.Error(w, err, http.StatusBadRequest)
		return
	}
	theJob, err := h.publishService.newJob(jobRequest.Concept, jobRequest.IDS, url, jobRequest.Throttle, jobRequest.Authorization)
	go h.publishService.runJob(theJob, jobRequest.Authorization)
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
	status, err := h.publishService.getJob(id)
	if err != nil {
		log.Errorf("message=\"Error returning job\" %v\n", err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(status); err != nil {
		log.Errorf("message=\"Error on json encoding\" %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *publishHandler) listJobs(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(h.publishService.getJobIds()); err != nil {
		log.Errorf("message=\"Error on json encoding\" %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
