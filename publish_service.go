package main

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/golang/go/src/pkg/bytes"
	"io"
	"math/rand"
	"net/url"
	"regexp"
	"sync"
	"time"
)

const messageTimestampDateFormat = "2006-01-02T15:04:05.000Z"
const loadBuffer = 128
const concurrentReaders = 128
const (
	defined    = "Defined"
	inProgress = "Defined"
	completed  = "Completed"
	failed     = "Failed"
)
const reloadSuffix = "__reload"
const idsSuffix = "__ids"
const countSuffix = "__count"

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var conceptTypeRegex, _ = regexp.Compile(`/([\w\-]+)/$`)

type job struct {
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

func (j job) String() string {
	return fmt.Sprintf("conceptType=%s url=%v count=%d throttle=%d status=%s progress=%d", j.ConceptType, j.URL, j.Count, j.Throttle, j.Status, j.Progress)
}

type concept struct {
	id      string
	payload []byte
}

type publishService struct {
	clusterRouterAddress *url.URL
	queueServiceI        *queueServiceI
	mutex                *sync.RWMutex //protects jobs
	jobs                 map[string]*job
	httpService          *httpServiceI
}

func newPublishService(clusterRouterAddress *url.URL, queueSer *queueServiceI, httpSer *httpServiceI) publishService {
	return publishService{
		clusterRouterAddress: clusterRouterAddress,
		queueServiceI:        queueSer,
		mutex:                &sync.RWMutex{},
		jobs:                 make(map[string]*job),
		httpService:          httpSer,
	}
}

type publishServiceI interface {
	newJob(ids []string, baseURL *url.URL, throttle int) (*job, error)
	getJob(jobID string) (*job, error)
	getJobStatus(jobID string) (string, error)
	getJobIds() []string
	runJob(theJob *job, authorization string)
	deleteJob(jobID string) error
}

func (s publishService) newJob(ids []string, baseURL *url.URL, throttle int) (*job, error) {
	jobID := "job_" + generateID()
	if baseURL.Host == "" {
		baseURL.Scheme = s.clusterRouterAddress.Scheme
		baseURL.Host = s.clusterRouterAddress.Host
	}
	idMap := make(map[string]string)
	for _, id := range ids {
		idMap[id] = ""
	}
	foundGroups := conceptTypeRegex.FindStringSubmatch(baseURL.Path)
	if len(foundGroups) < 2 {
		return nil, fmt.Errorf("message=\"Can't find concept type in URL. Must be like the following __special-reports-transformer/transformers/special-reports/  \" path=%s", baseURL.Path)
	}
	theJob := &job{
		JobID:       jobID,
		ConceptType: foundGroups[1],
		IDToTID:     idMap,
		URL:         *baseURL,
		Throttle:    throttle,
		Progress:    0,
		Status:      defined,
		FailedIDs:   []string{},
	}
	s.mutex.Lock()
	s.jobs[jobID] = theJob
	s.mutex.Unlock()
	log.Infof("message=\"Created job\" jobID=%s", theJob.JobID)
	return theJob, nil
}

func (s publishService) getJob(jobID string) (*job, error) {
	s.mutex.RLock()
	job, ok := s.jobs[jobID]
	if !ok {
		return nil, newNotFoundError(jobID)
	}
	s.mutex.RUnlock()
	return job, nil
}

func (s publishService) getJobStatus(jobID string) (string, error) {
	job, err := s.getJob(jobID)
	if err != nil {
		return "", err
	}
	return job.Status, nil
}

func (s publishService) getJobIds() []string {
	jobIds := []string{}
	s.mutex.RLock()
	for _, j := range s.jobs {
		jobIds = append(jobIds, j.JobID)
	}
	s.mutex.RUnlock()
	return jobIds
}

func (p publishService) runJob(theJob *job, authorization string) {
	theJob.Status = inProgress
	concepts := make(chan concept, loadBuffer)
	failures := make(chan failure, loadBuffer)
	err := (*p.httpService).reload(theJob.URL.String() + reloadSuffix, authorization)
	if err != nil {
		log.Infof("message=\"Couldn't reload concepts\" conceptType=\"%s\" %v", theJob.ConceptType, err)
	}
	if len(theJob.IDToTID) > 0 {
		theJob.Count = len(theJob.IDToTID)
	} else {
		theJob.Count, err = (*p.httpService).getCount(theJob.URL.String() + countSuffix, authorization)
		if err != nil {
			log.Warnf("message=\"Could not determine count for concepts. Job failed.\" conceptType=\"%s\" %v", theJob.ConceptType, err)
			theJob.Status = failed
			return
		}
	}
	go p.fetchAll(theJob, authorization, concepts, failures)
	for theJob.Progress = 0; theJob.Progress < theJob.Count; theJob.Progress++ {
		select {
		case f := <-failures:
			log.Warnf("message=\"failure at a concept, in a job\" jobID=%v conceptID=%v %v", theJob.JobID, f.conceptID, f.error)
			theJob.FailedIDs = append(theJob.FailedIDs, f.conceptID)
		case c := <-concepts:
			type shortPayload struct {
				UUID string `json:"uuid"`
			}
			var unmarshalledPayload shortPayload
			err = json.Unmarshal(c.payload, &unmarshalledPayload)
			if err != nil {
				log.Warnf("message=\"failed unmarshalling a concept\" jobID=%v conceptID=%v %v %v", theJob.JobID, c.id, string(c.payload), err)
				theJob.FailedIDs = append(theJob.FailedIDs, c.id)
			}
			resolvedID := unmarshalledPayload.UUID
			if c.id != resolvedID {
				log.Infof("message=\"initial uuid doesn't match fetched resolved uuid\" originalUuid=%v resolvedUuid=%v jobId=%v", c.id, resolvedID, theJob.JobID)
			}
			tid := "tid_" + generateID()
			theJob.IDToTID[resolvedID] = tid
			err := (*p.queueServiceI).sendMessage(resolvedID, theJob.ConceptType, tid, c.payload)
			if err != nil {
				log.Warnf("message=\"failed publishing a concept\" jobID=%v conceptID=%v %v", theJob.JobID, c.id, err)
				theJob.FailedIDs = append(theJob.FailedIDs, c.id)
			}
		}
	}
	theJob.Status = completed
	log.Infof("message=\"Completed job\" jobID=%s status=%s nFailedIds=%d", theJob.JobID, theJob.Status, len(theJob.FailedIDs))
}

func (s publishService) fetchAll(theJob *job, authorization string, concepts chan<- concept, failures chan<- failure) {
	ticker := time.NewTicker(time.Second / time.Duration(theJob.Throttle))
	idsChan := make(chan string, loadBuffer)
	if len(theJob.IDToTID) > 0 {
		go func() {
			for _, id := range theJob.IDToTID {
				idsChan <- id
			}
			close(idsChan)
		}()
	} else {
		go s.fetchIDList(theJob, authorization, idsChan, failures)
	}
	for i := 0; i < concurrentReaders; i++ {
		go s.fetchConcepts(theJob, authorization, concepts, idsChan, failures, ticker)
	}
	close(concepts)
}

func (p publishService) fetchIDList(theJob *job, authorization string, ids chan<- string, failures chan<- failure) {
	body, fail := (*p.httpService).getIds(theJob.URL.String() + idsSuffix, authorization)
	if fail != nil {
		fillFailures(fail, theJob.Count, failures)
		return
	}
	type listEntry struct {
		ID string `json:"id"`
	}
	var le listEntry
	dec := json.NewDecoder(bytes.NewReader(body))
	for {
		err := dec.Decode(&le)
		if err != nil {
			if err == io.EOF {
				break
			}
			fail := newFailure("", fmt.Errorf("Error parsing one concept id from /__ids response. url=%v %v", theJob.URL.String(), err))
			pushToFailures(fail, failures)
		}
		ids <- le.ID
	}
	close(ids)
}

func (p publishService) fetchConcepts(theJob *job, authorization string, concepts chan<- concept, ids <-chan string, failures chan<- failure, ticker *time.Ticker) {
	for id := range ids {
		<-ticker.C
		data, fail := (*p.httpService).fetchConcept(id, theJob.URL.String() + id, authorization)
		if fail != nil {
			pushToFailures(fail, failures)
			continue
		}
		concepts <- concept{id: id, payload: data}
	}
}

func (s publishService) deleteJob(jobID string) error {
	theJob, err := s.getJob(jobID)
	if err != nil {
		return err
	}
	if (theJob.Status != completed) && (theJob.Status != failed) {
		return newConflictError(jobID)
	}
	s.mutex.Lock()
	delete(s.jobs, jobID)
	s.mutex.Unlock()
	return nil
}

func pushToFailures(fail *failure, failures chan<- failure) {
	select {
	case failures <- *fail:
	default:
	}
}


func fillFailures(fail *failure, count int, failures chan<- failure) {
	for i := 0; i < count; i++ {
		pushToFailures(fail, failures)
	}
}

func generateID() string {
	b := make([]rune, 10)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
