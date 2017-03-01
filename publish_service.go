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
	"sync"
	"sync/atomic"
	"time"
)

const (
	messageTimestampDateFormat = "2006-01-02T15:04:05.000Z"
	loadBuffer                 = 128
	concurrentReaders          = 128

	defined    = "Defined"
	inProgress = "In Progress"
	completed  = "Completed"
	failed     = "Failed"

	reloadSuffix = "__reload"
	idsSuffix    = "__ids"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func (j job) String() string {
	return fmt.Sprintf("conceptType=%s url=%v gtgUrl=%v count=%d throttle=%d status=%s progress=%d", j.ConceptType, j.URL, j.GtgURL, j.Count, j.Throttle, j.Status, j.Progress)
}

type concept struct {
	id      string
	payload []byte
}

type publishService struct {
	sync.RWMutex
	clusterRouterAddress *url.URL
	queueService         *queue
	jobs                 map[string]*job
	httpService          *caller
	gtgRetries           int
}

func newPublishService(clusterRouterAddress *url.URL, queueService *queue, httpService *caller, gtgRetries int) publishService {
	return publishService{
		clusterRouterAddress: clusterRouterAddress,
		queueService:         queueService,
		jobs:                 make(map[string]*job),
		httpService:          httpService,
		gtgRetries:           gtgRetries,
	}
}

type publisher interface {
	createJob(conceptType string, ids []string, baseURL string, gtgURL string, throttle int) (*job, error)
	getJob(jobID string) (*job, error)
	getJobIds() []string
	runJob(theJob *job, authorization string)
	deleteJob(jobID string) error
}

func (s publishService) createJob(conceptType string, ids []string, baseURL string, gtgURL string, throttle int) (*job, error) {
	jobID := "job_" + generateID()
	baseURLParsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	if baseURLParsed.Host == "" {
		baseURLParsed.Scheme = s.clusterRouterAddress.Scheme
		baseURLParsed.Host = s.clusterRouterAddress.Host
	}
	gtgURLParsed, err := url.Parse(gtgURL)
	if err != nil {
		return nil, err
	}
	if gtgURLParsed.Host == "" {
		gtgURLParsed.Scheme = s.clusterRouterAddress.Scheme
		gtgURLParsed.Host = s.clusterRouterAddress.Host
	}
	theJob := &job{
		JobID:       jobID,
		ConceptType: conceptType,
		IDs:         ids,
		URL:         baseURLParsed.String(),
		GtgURL:      gtgURLParsed.String(),
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
	err := (*p.httpService).reload(theJob.URL+reloadSuffix, authorization)
	if err != nil {
		log.Infof("message=\"Couldn't reload concepts\" conceptType=\"%s\" %v", theJob.ConceptType, err)
	} else {
		gtgErr := p.pollGtg(theJob.GtgURL)
		if gtgErr != nil {
			log.Warnf("Timed out while waiting for transformer to reload. url=%v", theJob.GtgURL)
			theJob.updateStatus(failed)
			return
		}
	}
	var idsCount uint64 = 0
	var idsCounted uint64 = 0
	go p.fetchAll(theJob, authorization, &idsCounted, &idsCount, concepts, failures)
	for theJob.Progress = 0; atomic.LoadUint64(&idsCounted) == 0 || theJob.Progress < atomic.LoadUint64(&idsCount); theJob.incrementProgress() {
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
			err := (*p.queueService).sendMessage(theJob.ConceptType, theJob.JobID, c.payload)
			if err != nil {
				log.Warnf("message=\"failed publishing a concept\" jobID=%v conceptID=%v conceptType=%v %v", theJob.JobID, resolvedID, theJob.ConceptType, err)
				theJob.FailedIDs = append(theJob.FailedIDs, c.id)
			}
		}
	}
	theJob.updateStatus(completed)
	log.Infof("message=\"Completed job\" jobID=%s status=%s count=%d nFailedIds=%d", theJob.JobID, theJob.Status, theJob.Count, len(theJob.FailedIDs))
}

func (s publishService) fetchAll(theJob *job, authorization string, idsCounted *uint64, idsCount *uint64, concepts chan<- concept, failures chan<- failure) {
	ticker := time.NewTicker(time.Second / 1000)
	if theJob.Throttle > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(theJob.Throttle))
	}
	idsChan := make(chan string, loadBuffer)
	if len(theJob.IDs) > 0 {
		go func() {
			var ids []string
			theJob.RLock()
			ids = theJob.IDs
			theJob.RUnlock()
			for _, id := range ids {
				idsChan <- id
				atomic.AddUint64(idsCount, 1)
			}
			atomic.AddUint64(idsCounted, 1)
			theJob.updateCount(atomic.LoadUint64(idsCount))
			close(idsChan)
		}()
	} else {
		go s.fetchIDList(theJob, authorization, idsCounted, idsCount, idsChan, failures)
	}
	for i := 0; i < concurrentReaders; i++ {
		go s.fetchConcepts(theJob, authorization, concepts, idsChan, failures, ticker)
	}
}

func (p publishService) fetchIDList(theJob *job, authorization string, idsCounted *uint64, idsCount *uint64, ids chan<- string, failures chan<- failure) {
	body, fail := (*p.httpService).getIds(theJob.URL+idsSuffix, authorization)
	if fail != nil {
		pushToFailures(fail, failures)
		atomic.AddUint64(idsCounted, 1)
		atomic.AddUint64(idsCount, 1)
		close(ids)
		return
	}
	reader := bufio.NewReader(bytes.NewReader(body))
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Warnf("Error parsing one concept id from /__ids response. url=%v %v", theJob.URL, err)
			continue
		}
		atomic.AddUint64(idsCount, 1)
		reader2 := bufio.NewReader(bytes.NewReader(line))
		type listEntry struct {
			ID string `json:"id"`
		}
		var le listEntry
		dec := json.NewDecoder(reader2)
		err2 := dec.Decode(&le)
		if err2 != nil {
			fail := newFailure("", fmt.Errorf("Error parsing one concept id from /__ids response. url=%v %v", theJob.URL, err2))
			pushToFailures(fail, failures)
			continue
		}
		ids <- le.ID
	}
	atomic.AddUint64(idsCounted, 1)
	theJob.updateCount(atomic.LoadUint64(idsCount))
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
		data, fail := (*p.httpService).fetchConcept(id, theJob.URL+id, authorization)
		if fail != nil {
			log.Warnf("coulnd't fetch concept, putting it to failures %v", id)
			pushToFailures(fail, failures)
			continue
		}
		concepts <- concept{id: id, payload: data}
	}
}

func (s publishService) deleteJob(jobID string) error {
	_, err := s.getJob(jobID)
	if err != nil {
		return err
	}
	s.Lock()
	defer s.Unlock()
	delete(s.jobs, jobID)
	return nil
}

func (p publishService) pollGtg(gtgUrl string) error {
	log.Infof("Waiting on transformer to be good to go. url=%s", gtgUrl)
	for i := 0; i < p.gtgRetries; i++ {
		gtgErr := (*p.httpService).checkGtg(gtgUrl)
		if gtgErr != nil {
			time.Sleep(time.Second * 5)
		} else {
			return nil
		}
	}
	return fmt.Errorf("Timed out. Did you put the correct gtgUrl field on your create job request? url=%v", gtgUrl)
}

func pushToFailures(fail *failure, failures chan<- failure) {
	select {
	case failures <- *fail:
	}
}

func generateID() string {
	b := make([]rune, 10)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
