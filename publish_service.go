package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io"
	"math/rand"
	"net/url"
	"reflect"
	"regexp"
	"sync"
	"time"
)

const (
	messageTimestampDateFormat = "2006-01-02T15:04:05.000Z"
	loadBuffer                 = 128
	concurrentReaders          = 128

	defined    = "Defined"
	inProgress = "Defined"
	completed  = "Completed"
	failed     = "Failed"

	reloadSuffix = "__reload"
	idsSuffix    = "__ids"
	countSuffix  = "__count"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var conceptTypeRegex = regexp.MustCompile(`/([\w\-]+)/$`)

func (j job) String() string {
	return fmt.Sprintf("conceptType=%s url=%v count=%d throttle=%d status=%s progress=%d", j.ConceptType, j.URL, j.Count, j.Throttle, j.Status, j.Progress)
}

type concept struct {
	id      string
	payload []byte
}

type publishService struct {
	sync.RWMutex
	clusterRouterAddress *url.URL
	queueServiceI        *queueServiceI
	jobs                 map[string]*job
	httpService          *httpServiceI
}

func newPublishService(clusterRouterAddress *url.URL, queueSer *queueServiceI, httpSer *httpServiceI) publishService {
	return publishService{
		clusterRouterAddress: clusterRouterAddress,
		queueServiceI:        queueSer,
		jobs:                 make(map[string]*job),
		httpService:          httpSer,
	}
}

type publishServiceI interface {
	createJob(ids []string, baseURL url.URL, throttle int) (*job, error)
	getJob(jobID string) (*job, error)
	getJobIds() []string
	runJob(theJob *job, authorization string)
	deleteJob(jobID string) error
}

func (s publishService) createJob(ids []string, baseURL url.URL, throttle int) (*job, error) {
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
		URL:         baseURL,
		Throttle:    throttle,
		Progress:    0,
		Status:      defined,
		FailedIDs:   []string{},
	}
	s.Lock()
	defer s.Unlock()
	s.jobs[jobID] = theJob
	log.Infof("message=\"Created job\" jobID=%s", theJob.JobID)
	return theJob, nil
}

func (s publishService) getJob(jobID string) (*job, error) {
	s.RLock()
	defer s.RUnlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return nil, newNotFoundError(jobID)
	}
	return job, nil
}

func (s publishService) getJobIds() []string {
	jobIds := []string{}
	s.RLock()
	defer s.RUnlock()
	for _, j := range s.jobs {
		j.RLock()
		jobIds = append(jobIds, j.JobID)
		j.RUnlock()
	}
	return jobIds
}

func (p publishService) runJob(theJob *job, authorization string) {
	theJob.updateStatus(inProgress)
	concepts := make(chan concept, loadBuffer)
	failures := make(chan failure, loadBuffer)
	err := (*p.httpService).reload(theJob.URL.String()+reloadSuffix, authorization)
	if err != nil {
		log.Infof("message=\"Couldn't reload concepts\" conceptType=\"%s\" %v", theJob.ConceptType, err)
	}
	var jobCount int
	if len(theJob.IDToTID) > 0 {
		jobCount = len(theJob.IDToTID)
	} else {
		jobCount, err = (*p.httpService).getCount(theJob.URL.String()+countSuffix, authorization)
		if err != nil {
			log.Warnf("message=\"Could not determine count for concepts. Job failed.\" conceptType=\"%s\" %v", theJob.ConceptType, err)
			theJob.updateStatus(failed)
			return
		}
	}
	theJob.updateCount(jobCount)
	go p.fetchAll(theJob, authorization, concepts, failures)
	for theJob.Progress = 0; theJob.Progress < jobCount; theJob.updateProgress() {
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
				log.Warnf("message=\"failed unmarshalling a concept\" jobID=%v conceptID=%v payload=%v %v", theJob.JobID, c.id, string(c.payload), err)
				theJob.FailedIDs = append(theJob.FailedIDs, c.id)
			}
			resolvedID := unmarshalledPayload.UUID
			if !reflect.DeepEqual(c.id, resolvedID) {
				log.Infof("message=\"initial uuid doesn't match fetched resolved uuid\" originalUuid=%v resolvedUuid=%v jobId=%v", c.id, resolvedID, theJob.JobID)
			}
			tid := "tid_" + generateID()

			// lock job to update
			theJob.Lock()
			theJob.IDToTID[resolvedID] = tid
			err := (*p.queueServiceI).sendMessage(resolvedID, theJob.ConceptType, tid, c.payload)
			if err != nil {
				log.Warnf("message=\"failed publishing a concept\" jobID=%v conceptID=%v %v", theJob.JobID, c.id, err)
				theJob.FailedIDs = append(theJob.FailedIDs, c.id)
			}
			theJob.Unlock()
		}
	}

	theJob.updateStatus(completed)
	log.Infof("message=\"Completed job\" jobID=%s status=%s nFailedIds=%d", theJob.JobID, theJob.Status, len(theJob.FailedIDs))
}

func (s publishService) fetchAll(theJob *job, authorization string, concepts chan<- concept, failures chan<- failure) {
	ticker := time.NewTicker(time.Second / 1000)
	if theJob.Throttle > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(theJob.Throttle))
	}
	idsChan := make(chan string, loadBuffer)
	if len(theJob.IDToTID) > 0 {
		go func() {
			theJob.RLock()
			defer theJob.RUnlock()
			for id := range theJob.IDToTID {
				idsChan <- id
			}
			close(idsChan)
		}()
	} else {
		s.fetchIDList(theJob, authorization, idsChan, failures)
	}
	for i := 0; i < concurrentReaders; i++ {
		go s.fetchConcepts(theJob, authorization, concepts, idsChan, failures, ticker)
	}
}

func (p publishService) fetchIDList(theJob *job, authorization string, ids chan<- string, failures chan<- failure) {
	body, fail := (*p.httpService).getIds(theJob.URL.String()+idsSuffix, authorization)
	if fail != nil {
		fillFailures(fail, theJob.Count, failures)
		return
	}
	reader := bufio.NewReader(bytes.NewReader(body))
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Warnf("Error parsing one concept id from /__ids response. url=%v %v", theJob.URL.String(), err)
			continue
		}
		reader2 := bufio.NewReader(bytes.NewReader(line))
		type listEntry struct {
			ID string `json:"id"`
		}
		var le listEntry
		dec := json.NewDecoder(reader2)
		err2 := dec.Decode(&le)
		if err2 != nil {
			fail := newFailure("", fmt.Errorf("Error parsing one concept id from /__ids response. url=%v %v", theJob.URL.String(), err2))
			pushToFailures(fail, failures)
			continue
		}
		ids <- le.ID
	}
	close(ids)
}

func (p publishService) fetchConcepts(theJob *job, authorization string, concepts chan<- concept, ids <-chan string, failures chan<- failure, ticker *time.Ticker) {
	for {
		id, ok := <-ids
		if !ok {
			break
		}
		if theJob.Throttle > 0 {
			<-ticker.C
		}
		data, fail := (*p.httpService).fetchConcept(id, theJob.URL.String()+id, authorization)
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
	theJob.RLock()
	defer theJob.RUnlock()
	if (theJob.Status != completed) && (theJob.Status != failed) && (theJob.Status != defined) {
		return newConflictError(jobID)
	}
	s.Lock()
	defer s.Unlock()
	delete(s.jobs, jobID)
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
