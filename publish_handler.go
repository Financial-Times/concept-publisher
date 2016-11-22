package main

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

type publishHandler struct {
	publishService *publishService
}

type createJobRequest struct {
	URL           string   `json:"url"`
	Throttle      int      `json:"throttle"`
	Authorization string   `json:"authorization"`
	IDS           []string `json:"ids"`
}

func newPublishHandler(publishService *publishService) publishHandler {
	return publishHandler{publishService: publishService}
}

func (j createJobRequest) String() string {
	return fmt.Sprintf("URL=\"%s\" Throttle=%d", j.URL, j.Throttle)
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
	theJob, err := h.publishService.newJob(jobRequest.IDS, url, jobRequest.Throttle, jobRequest.Authorization)
	if err != nil {
		log.Errorf("%v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	type shortJob struct {
		JobID string `json:"jobID"`
	}
	sj := shortJob{JobID: theJob.JobID}
	enc := json.NewEncoder(w)
	err = enc.Encode(sj)
	if err != nil {
		log.Errorf("Error on json encoding=%v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	go h.publishService.runJob(theJob, jobRequest.Authorization)
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

func (h *publishHandler) deleteJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	err := h.publishService.deleteJob(id)
	if err != nil {
		_, ok := err.(*notFoundError)
		code := http.StatusBadRequest
		if ok {
			code = http.StatusNotFound
		}
		_, ok = err.(*conflictError)
		if ok {
			code = http.StatusConflict
		}
		log.Infof("message=\"Error deleting job\" %v\n", err)
		http.Error(w, err.Error(), code)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func closeNice(resp *http.Response) {
	_, err := io.Copy(ioutil.Discard, resp.Body)
	if err != nil {
		log.Warnf("message=\"couldn't read response body\" %v", err)
	}
	err = resp.Body.Close()
	if err != nil {
		log.Warnf("message=\"couldn't close response body\" %v", err)
	}
}
