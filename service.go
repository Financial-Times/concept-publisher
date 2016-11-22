package main

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/message-queue-go-producer/producer"
	log "github.com/Sirupsen/logrus"
	"github.com/golang/go/src/pkg/bytes"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
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

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var conceptTypeRegex, _ = regexp.Compile(`/([\w\-]+)/$"`)

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

type failure struct {
	conceptID string
	error     error
}

func (j job) String() string {
	return fmt.Sprintf("conceptType=%s url=%v count=%d throttle=%d status=%s progress=%d", j.ConceptType, j.URL, j.Count, j.Throttle, j.Status, j.Progress)
}

type concept struct {
	id      string
	payload []byte
}

type pubService interface {
	newJob(concept string, ids []string, baseURL *url.URL, authorization string, throttle int) (*job, error)
	getJob(jobID string) (*job, notFoundError)
	getJobStatus(jobID string) (string, notFoundError)
	getJobIds() []string
	runJob(theJob *job, authorization string)
}

type publishService struct {
	clusterRouterAddress *url.URL
	producer             producer.MessageProducer
	mutex                *sync.RWMutex //protects jobs
	jobs                 map[string]*job
	httpClient           *http.Client
}

func newPublishService(clusterRouterAddress *url.URL, producer producer.MessageProducer, httpClient *http.Client) publishService {
	return publishService{
		clusterRouterAddress: clusterRouterAddress,
		producer:             producer,
		mutex:                &sync.RWMutex{},
		jobs:                 make(map[string]*job),
		httpClient:           httpClient,
	}
}

func (s *publishService) newJob(ids []string, baseURL *url.URL, throttle int, authorization string) (*job, error) {
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
		return nil, errors.New("Can't find concept type in URL")
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

func (s *publishService) getJob(jobID string) (*job, error) {
	s.mutex.RLock()
	job, ok := s.jobs[jobID]
	if !ok {
		return nil, newNotFoundError(jobID)
	}
	s.mutex.RUnlock()
	return job, nil
}

func (s *publishService) getJobStatus(jobID string) (string, error) {
	job, err := s.getJob(jobID)
	if err != nil {
		return "", err
	}
	return job.Status, nil
}

func (s *publishService) getJobIds() []string {
	jobIds := []string{}
	s.mutex.RLock()
	for _, j := range s.jobs {
		jobIds = append(jobIds, j.JobID)
	}
	s.mutex.RUnlock()
	return jobIds
}

func (s *publishService) runJob(theJob *job, authorization string) {
	theJob.Status = inProgress
	concepts := make(chan concept, loadBuffer)
	failures := make(chan failure, loadBuffer)
	err := s.reload(theJob.URL, authorization)
	if err != nil {
		log.Infof("message=\"Couldn't reload concepts\" conceptType=\"%s\" %v", theJob.ConceptType, err)
	}
	count, err := s.getCount(theJob.IDToTID, theJob.URL, authorization)
	if err != nil {
		log.Warnf("message=\"Could not determine count for concepts. Job failed.\" conceptType=\"%s\" %v", theJob.ConceptType, err)
		theJob.Status = failed
		return
	}
	theJob.Count = count
	go func() {
		s.fetchAll(theJob, authorization, concepts, failures)
		close(concepts)
	}()
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
			message := producer.Message{
				Headers: buildHeader(resolvedID, theJob.ConceptType),
				Body:    string(c.payload),
			}
			theJob.IDToTID[resolvedID] = message.Headers["X-Request-Id"]
			err := s.producer.SendMessage(c.id, message)
			if err != nil {
				log.Warnf("message=\"failed publishing a concept\" jobID=%v conceptID=%v %v", theJob.JobID, c.id, err)
				theJob.FailedIDs = append(theJob.FailedIDs, c.id)
			}
		}
	}
	theJob.Status = completed
	log.Infof("message=\"Completed job\" jobID=%s status=%s nFailedIds=%d", theJob.JobID, theJob.Status, len(theJob.FailedIDs))
}

func (s *publishService) fetchAll(theJob *job, authorization string, concepts chan<- concept, failures chan<- failure) {
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
		s.fetchConcepts(theJob, authorization, concepts, idsChan, failures, ticker)
	}
}

func (s *publishService) fetchIDList(theJob *job, authorization string, ids chan<- string, failures chan<- failure) {
	body := s.fetchIdsFromRemote(theJob, authorization, failures)
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
			select {
			case failures <- failure{
				conceptID: "",
				error:     fmt.Errorf("Error parsing one concept id from /__ids response. url=%v %v", theJob.URL.String(), err),
			}:
			default:
			}
		}
		ids <- le.ID
	}
	close(ids)
}

func (s *publishService) fetchIdsFromRemote(theJob *job, authorization string, failures chan<- failure) []byte {
	req, _ := http.NewRequest("GET", theJob.URL.String()+"__ids", nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		fillFailures(failure{
			conceptID: "",
			error:     fmt.Errorf("Could not get /__ids from: %v (%v)", theJob.URL, err),
		}, theJob.Count, failures)
	}
	defer closeNice(resp)
	if resp.StatusCode != http.StatusOK {
		fillFailures(failure{
			conceptID: "",
			error:     fmt.Errorf("Could not get /__ids from %v. Returned %v", theJob.URL, resp.StatusCode),
		}, theJob.Count, failures)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fillFailures(failure{
			conceptID: "",
			error:     fmt.Errorf("message=\"Could not read /__ids response\" %v", err),
		}, theJob.Count, failures)
	}
	return body
}

func fillFailures(fail failure, count int, failures chan<- failure) {
	for i := 0; i < count; i++ {
		select {
		case failures <- fail:
		default:
		}
	}
}

func (s *publishService) fetchConcepts(theJob *job, authorization string, concepts chan<- concept, ids <-chan string, failures chan<- failure, ticker *time.Ticker) {
	for id := range ids {
		<-ticker.C
		req, _ := http.NewRequest("GET", theJob.URL.String()+id, nil)
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		resp, err := s.httpClient.Do(req)
		if err != nil {
			pushToFailures(failure{
				conceptID: id,
				error:     fmt.Errorf("message=\"Could not make HTTP request to fetch a concept\" conceptId=%v %v", id, err),
			}, failures)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			pushToFailures(failure{
				conceptID: id,
				error:     fmt.Errorf("message=\"Fetching a concept resulted in not ok response\" conceptId=%v jobId=%v status=%v", id, theJob.URL, resp.StatusCode),
			}, failures)
			continue
		}
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			pushToFailures(failure{
				conceptID: id,
				error:     fmt.Errorf("message=\"Could not read concept from response while fetching\" uuid=%v %v", id, err),
			}, failures)
			continue
		}
		err = resp.Body.Close()
		if err != nil {
			pushToFailures(failure{
				conceptID: id,
				error:     fmt.Errorf("message=\"Could not close response while fetching\" uuid=%v %v", id, err),
			}, failures)
			continue
		}
		concepts <- concept{id: id, payload: data}
	}
}

func pushToFailures(fail failure, failures chan<- failure) {
	select {
	case failures <- fail:
	default:
	}
}

func (s *publishService) deleteJob(jobID string) error {
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

func (s *publishService) getCount(idToTID map[string]string, baseURL url.URL, authorization string) (int, error) {
	if len(idToTID) > 0 {
		return len(idToTID), nil
	}
	req, _ := http.NewRequest("GET", baseURL.String()+"__count", nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return -1, fmt.Errorf("Could not connect to %v. Error (%v)", baseURL, err)
	}
	defer closeNice(resp)
	if resp.StatusCode != http.StatusOK {
		return -1, fmt.Errorf("Could not get count from %v. Returned %v", baseURL, resp.StatusCode)
	}
	p, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return -1, fmt.Errorf("Could not read count from %v. Error (%v)", baseURL, err)
	}
	c, err := strconv.Atoi(string(p))
	if err != nil {
		return -1, fmt.Errorf("Could not convert payload (%v) to int. Error (%v)", string(p), err)
	}
	return c, nil
}

func (s *publishService) reload(baseURL url.URL, authorization string) error {
	url := baseURL.String() + reloadSuffix
	req, _ := http.NewRequest("POST", url, nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("message=\"Could not connect to reload concepts\" url=\"%v\" err=\"%s\"", url, err)
	}
	defer closeNice(resp)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("message=\"Incorrect status when reloading concepts\" status=%d url=\"%s\"", resp.StatusCode, url)
	}
	return nil
}

func buildHeader(uuid string, conceptType string) map[string]string {
	return map[string]string{
		"Message-Id":        uuid,
		"Message-Type":      conceptType,
		"Content-Type":      "application/json",
		"X-Request-Id":      "tid_" + generateID(),
		"Origin-System-Id":  "http://cmdb.ft.com/systems/upp",
		"Message-Timestamp": time.Now().Format(messageTimestampDateFormat),
	}
}

func generateID() string {
	b := make([]rune, 10)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
