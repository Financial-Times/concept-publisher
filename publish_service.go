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
	jobs                 map[string]*internalJob
	httpService          *caller
	gtgRetries           int
}

func newPublishService(clusterRouterAddress *url.URL, queueService *queue, httpService *caller, gtgRetries int) *publishService {
	return &publishService{
		clusterRouterAddress: clusterRouterAddress,
		queueService: queueService,
		jobs:         make(map[string]*internalJob),
		httpService:  httpService,
		gtgRetries:   gtgRetries,
	}
}

type publisher interface {
	createJob(conceptType string, ids []string, baseURL string, gtgURL string, throttle int) (*internalJob, error)
	getJob(jobID string) (*internalJob, error)
	getJobIds() []string
	runJob(theJob *internalJob, authorization string)
	deleteJob(jobID string) error
}

func (p *publishService) createJob(conceptType string, ids []string, baseURL string, gtgURL string, throttle int) (*internalJob, error) {
	jobID := "job_" + generateID()
	baseURLParsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	gtgURLParsed, err := url.Parse(gtgURL)
	if err != nil {
		return nil, err
	}
	if p.clusterRouterAddress != nil{
		baseURLParsed.Scheme = p.clusterRouterAddress.Scheme
		baseURLParsed.Host = p.clusterRouterAddress.Host
		gtgURLParsed.Scheme = p.clusterRouterAddress.Scheme
		gtgURLParsed.Host = p.clusterRouterAddress.Host
	}
	theJob := &internalJob{
		jobID:       jobID,
		conceptType: conceptType,
		ids:         ids,
		url:         baseURLParsed.String(),
		gtgURL:      gtgURLParsed.String(),
		throttle:    throttle,
		progress:    0,
		status:      defined,
		failedIDs:   []string{},
	}
	p.Lock()
	defer p.Unlock()
	p.jobs[jobID] = theJob
	log.Infof("message=\"Created job\" jobID=%s", theJob.jobID)
	return theJob, nil
}

func (p *publishService) getJob(jobID string) (*internalJob, error) {
	p.RLock()
	defer p.RUnlock()
	job, ok := p.jobs[jobID]
	if !ok {
		return nil, newNotFoundError(jobID)
	}
	return job, nil
}

func (p *publishService) getJobIds() []string {
	jobIds := []string{}
	p.RLock()
	defer p.RUnlock()
	for _, j := range p.jobs {
		jobIds = append(jobIds, j.jobID)
	}
	return jobIds
}

func (p *publishService) runJob(theJob *internalJob, authorization string) {
	theJob.updateStatus(inProgress)
	concepts := make(chan concept, loadBuffer)
	failures := make(chan failure, loadBuffer)
	err := (*p.httpService).reload(theJob.url+reloadSuffix, authorization)
	if err != nil {
		log.Infof("message=\"Couldn't reload concepts\" conceptType=\"%s\" %v", theJob.conceptType, err)
	} else {
		gtgErr := p.pollGtg(theJob.gtgURL)
		if gtgErr != nil {
			log.Warnf("Timed out while waiting for transformer to reload. url=%v", theJob.gtgURL)
			theJob.updateStatus(failed)
			return
		}
	}
	var idsCount uint64
	var idsCounted uint64
	go p.fetchAll(theJob, authorization, &idsCounted, &idsCount, concepts, failures)
	for theJob.setProgress(0); atomic.LoadUint64(&idsCounted) == 0 || theJob.getProgress() < atomic.LoadUint64(&idsCount); theJob.incrementProgress() {
		select {
		case f := <-failures:
			log.Warnf("message=\"failure at a concept, in a job\" jobID=%v conceptID=%v %v", theJob.jobID, f.conceptID, f.error)
			theJob.appendFailedID(f.conceptID)
		case c := <-concepts:
			type shortPayload struct {
				UUID string `json:"uuid"`
			}
			var unmarshalledPayload shortPayload
			err = json.Unmarshal(c.payload, &unmarshalledPayload)
			if err != nil {
				log.Warnf("message=\"failed unmarshalling a concept\" jobID=%v conceptID=%v payload=%v %v", theJob.jobID, c.id, string(c.payload), err)
				theJob.appendFailedID(c.id)
			}
			resolvedID := unmarshalledPayload.UUID
			if !reflect.DeepEqual(c.id, resolvedID) {
				log.Infof("message=\"initial uuid doesn't match fetched resolved uuid\" originalUuid=%v resolvedUuid=%v jobId=%v", c.id, resolvedID, theJob.jobID)
			}
			err := (*p.queueService).sendMessage(resolvedID, theJob.conceptType, theJob.jobID, c.payload)
			if err != nil {
				log.Warnf("message=\"failed publishing a concept\" jobID=%v conceptID=%v conceptType=%v %v", theJob.jobID, resolvedID, theJob.conceptType, err)
				theJob.appendFailedID(c.id)
			}
		}
	}
	theJob.updateStatus(completed)
	log.Infof("message=\"Completed job\" jobID=%s status=%s count=%d nFailedIds=%d", theJob.jobID, theJob.getStatus(), theJob.getCount(), len(theJob.getFailedIDs()))
}

func (p *publishService) fetchAll(theJob *internalJob, authorization string, idsCounted *uint64, idsCount *uint64, concepts chan<- concept, failures chan<- failure) {
	ticker := time.NewTicker(time.Second / 1000)
	if theJob.throttle > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(theJob.throttle))
	}
	idsChan := make(chan string, loadBuffer)
	if len(theJob.ids) > 0 {
		go func() {
			var ids []string
			theJob.RLock()
			ids = theJob.ids
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
		go p.fetchIDList(theJob, authorization, idsCounted, idsCount, idsChan, failures)
	}
	for i := 0; i < concurrentReaders; i++ {
		go p.fetchConcepts(theJob, authorization, concepts, idsChan, failures, ticker)
	}
}

func (p *publishService) fetchIDList(theJob *internalJob, authorization string, idsCounted *uint64, idsCount *uint64, ids chan<- string, failures chan<- failure) {
	body, fail := (*p.httpService).getIds(theJob.url+idsSuffix, authorization)
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
			log.Warnf("Error parsing one concept id from /__ids response. url=%v %v", theJob.url, err)
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
			fail := newFailure("", fmt.Errorf("Error parsing one concept id from /__ids response. url=%v %v", theJob.url, err2))
			pushToFailures(fail, failures)
			continue
		}
		ids <- le.ID
	}
	atomic.AddUint64(idsCounted, 1)
	theJob.updateCount(atomic.LoadUint64(idsCount))
	close(ids)
}

func (p *publishService) fetchConcepts(theJob *internalJob, authorization string, concepts chan<- concept, ids <-chan string, failures chan<- failure, ticker *time.Ticker) {
	for {
		id, ok := <-ids
		if !ok {
			break
		}
		if theJob.throttle > 0 {
			<-ticker.C
		}
		data, fail := (*p.httpService).fetchConcept(id, theJob.url+id, authorization)
		if fail != nil {
			log.Warnf("couldn't fetch concept, putting it to failures %v", id)
			pushToFailures(fail, failures)
			continue
		}
		concepts <- concept{id: id, payload: data}
	}
}

func (p *publishService) deleteJob(jobID string) error {
	_, err := p.getJob(jobID)
	if err != nil {
		return err
	}
	p.Lock()
	defer p.Unlock()
	delete(p.jobs, jobID)
	return nil
}

func (p *publishService) pollGtg(gtgUrl string) error {
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
