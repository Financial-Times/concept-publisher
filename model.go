package main

import (
	"sync"
)

type job struct {
	sync.RWMutex
	JobID       string            `json:"jobID"`
	ConceptType string            `json:"conceptType"`
	IDs         []string          `json:"IDToTID,omitempty"`
	URL         string            `json:"url"`
	GtgURL      string            `json:"gtgUrl"`
	Throttle    int               `json:"throttle"`
	Count       int               `json:"count"`
	Progress    int               `json:"progress"`
	Status      string            `json:"status"`
	FailedIDs   []string          `json:"failedIDs,omitempty"`
}

type createJobRequest struct {
	ConceptType   string   `json:"concept"`
	URL           string   `json:"url"`
	GtgURL        string   `json:"gtgUrl"`
	Throttle      int      `json:"throttle"`
	Authorization string   `json:"authorization"`
	IDS           []string `json:"ids"`
}

func (theJob *job) updateStatus(status string) {
	theJob.Lock()
	theJob.Status = status
	theJob.Unlock()
}

func (theJob *job) updateCount(count int) {
	theJob.Lock()
	theJob.Count = count
	theJob.Unlock()
}

func (theJob *job) updateProgress() {
	theJob.Lock()
	theJob.Progress++
	theJob.Unlock()
}
