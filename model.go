package main

import (
	"net/url"
	"sync"
	log "github.com/Sirupsen/logrus"
)

type job struct {
	sync.RWMutex
	JobID       string            `json:"jobID"`
	ConceptType string            `json:"conceptType"`
	IDToTID     map[string]string `json:"IDToTID,omitempty"`
	URL         url.URL           `json:"url"`
	Throttle    int               `json:"throttle"`
	Count       int               `json:"count"`
	Progress    int               `json:"progress"`
	Status      string            `json:"status"`
	FailedIDs   []string          `json:"failedIDs"`
}

type createJobRequest struct {
	URL           string   `json:"url"`
	Throttle      int      `json:"throttle"`
	Authorization string   `json:"authorization"`
	IDS           []string `json:"ids"`
}

func (theJob *job) updateStatus(status string) {
	log.Infof("Locking %v because status", theJob.JobID)
	theJob.Lock()
	theJob.Status = status
	log.Infof("Unlocking %v because status", theJob.JobID)
	theJob.Unlock()
}

func (theJob *job) updateCount(count int) {
	log.Infof("Locking %v because count", theJob.JobID)
	theJob.Lock()
	theJob.Count = count
	log.Infof("Unlocking %v because count", theJob.JobID)
	theJob.Unlock()
}

func (theJob *job) updateProgress() {
	log.Infof("Locking %v because progress", theJob.JobID)
	theJob.Lock()
	theJob.Progress++
	log.Infof("Unlocking %v because progress", theJob.JobID)
	theJob.Unlock()
}
