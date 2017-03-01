package main

import (
	"sync"
	"sync/atomic"
)

type job struct {
	JobID       string   `json:"jobID"`
	ConceptType string   `json:"conceptType"`
	IDs         []string `json:"IDToTID,omitempty"`
	URL         string   `json:"url"`
	GtgURL      string   `json:"gtgUrl"`
	Throttle    int      `json:"throttle"`
	Count       uint64   `json:"count"`
	Progress    uint64   `json:"progress"`
	Status      string   `json:"status"`
	FailedIDs   []string `json:"failedIDs,omitempty"`
}

type createJobRequest struct {
	ConceptType   string   `json:"concept"`
	URL           string   `json:"url"`
	GtgURL        string   `json:"gtgUrl"`
	Throttle      int      `json:"throttle"`
	Authorization string   `json:"authorization"`
	IDS           []string `json:"ids"`
}

type internalJob struct {
	sync.RWMutex
	jobID       string
	conceptType string
	ids         []string
	url         string
	gtgURL      string
	throttle    int
	count       uint64
	progress    uint64
	status      string
	failedIDs   []string
}

func (j *internalJob) updateStatus(status string) {
	j.Lock()
	j.status = status
	j.Unlock()
}

func (j *internalJob) getProgress() uint64 {
	return atomic.LoadUint64(&j.progress)
}

func (j *internalJob) setProgress(i uint64) {
	atomic.StoreUint64(&j.progress, i)
}

func (j *internalJob) getStatus() string {
	j.RLock()
	defer j.RUnlock()
	return j.status
}

func (j *internalJob) updateCount(count uint64) {
	j.Lock()
	j.count = count
	j.Unlock()
}

func (j *internalJob) getCount() uint64 {
	return atomic.LoadUint64(&j.count)
}

func (j *internalJob) appendFailedID(id string) {
	j.Lock()
	j.failedIDs = append(j.failedIDs, id)
	j.Unlock()
}

func (j *internalJob) getFailedIDs() []string {
	j.RLock()
	defer j.RUnlock()
	return j.failedIDs
}

func (j *internalJob) incrementProgress() {
	atomic.AddUint64(&j.progress, 1)
}

func (j *internalJob) getJobFiltered() *job {
	j.RLock()
	defer j.RUnlock()
	return &job{
		JobID:       j.jobID,
		ConceptType: j.conceptType,
		Count:       j.getCount(),
		Progress:    j.getProgress(),
		Status:      j.status,
		Throttle:    j.throttle,
		URL:         j.url,
		GtgURL:      j.gtgURL,
	}

}

func (j *internalJob) getJob() *job {
	j.RLock()
	defer j.RUnlock()
	return &job{
		JobID:       j.jobID,
		ConceptType: j.conceptType,
		IDs:         j.ids,
		Count:       j.getCount(),
		Progress:    j.getProgress(),
		Status:      j.status,
		Throttle:    j.throttle,
		URL:         j.url,
		GtgURL:      j.gtgURL,
		FailedIDs:   j.failedIDs,
	}

}
