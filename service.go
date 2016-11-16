package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/Financial-Times/message-queue-go-producer/producer"
	log "github.com/Sirupsen/logrus"
)

const messageTimestampDateFormat = "2006-01-02T15:04:05.000Z"
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
const loadBuffer = 128
const concurrentReaders = 128
const (
	defined = "Defined"
	inProgress = "Defined"
	completed = "Completed"
)

type job struct {
	JobID       string   `json:"jobId"`
	ConceptType string   `json:"conceptType"`
	IDs         []string `json:"ids,omitempty"`
	URL         url.URL  `json:"url"`
	Throttle    int      `json:"throttle"`
	Count       int      `json:"count"`
	Progress    int      `json:"progress"`
	Status      string   `json:"status"`
	FailedIDs   []string `json:"failedIDs"`
}

type failure struct {
	conceptId string
	error     error
}

func (j job) String() string {
	return fmt.Sprintf("conceptType=%s url=%s count=%d throttle=%d status=%s progress=%d", j.ConceptType, j.URL, j.Count, j.Throttle, j.Status, j.Progress)
}

type notFoundError struct {
	msg string
}

func newNotFoundError(jobID string) *notFoundError {
	return &notFoundError{msg: fmt.Sprintf("Job with id=%s not found", jobID)}
}

func (e *notFoundError) Error() string {
	return e.msg
}

type concept struct {
	id      string
	payload string
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
		jobs: 	              make(map[string]*job),
		httpClient:           httpClient,
	}
}

func (s *publishService) newJob(conceptType string, ids []string, baseURL *url.URL, authorization string, throttle int) (*job, error) {
	if baseURL.Host == "" {
		baseURL.Scheme = s.clusterRouterAddress.Scheme
		baseURL.Host = s.clusterRouterAddress.Host
	}
	err := s.reload(baseURL, authorization)
	if err != nil {
		log.Infof("Couldn't reload concept=%s %v", conceptType, err)
	}
	count, err := s.getCount(ids, baseURL, authorization)
	if err != nil {
		return nil, fmt.Errorf("Could not determine count for concept=%s %v", conceptType, err)
	}
	jobID := "job_" + generateID()
	theJob := &job{
		ConceptType: conceptType,
		IDs: ids,
		URL: *baseURL,
		Throttle: throttle,
		Count: count,
		Progress: 0,
		Status: defined,
		FailedIDs: []string{},
	}
	s.mutex.Lock()
	s.jobs[jobID] = theJob
	s.mutex.Unlock()
	log.Infof("Created job with jobID=%s", theJob.JobID)
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
	concepts := make(chan concept, loadBuffer)
	failures := make(chan failure, loadBuffer)
	go func() {
		s.fetchAll(theJob, authorization, concepts, failures)
		close(concepts)
	}()
	theJob.Status = inProgress
	for theJob.Progress = 0; theJob.Progress < theJob.Count; theJob.Progress++ {
		select {
		case f := <-failures:
			log.Warnf("jobID=%v failed fetching conceptID=%v in %v", theJob.JobID, f.conceptId, f.error)
			theJob.FailedIDs = append(theJob.FailedIDs, f.conceptId)
		case c := <-concepts:
			message := producer.Message{
				Headers: buildHeader(c.id, theJob.ConceptType),
				Body: c.payload,
			}
			err := s.producer.SendMessage(c.id, message)
			if err != nil {
				log.Warnf("jobID=%v failed publishing conceptID=%v in %v", theJob.JobID, c.id, err)
				theJob.FailedIDs = append(theJob.FailedIDs, c.id)
			}
		}
	}
	theJob.Status = completed
	log.Infof("Completed job with jobID=%s", theJob.JobID)
}

func (s *publishService) fetchAll(theJob *job, authorization string, concepts chan<- concept, failures chan<- failure) {
	ticker := time.NewTicker(time.Second / time.Duration(theJob.Throttle))
	idsChan := make(chan string, loadBuffer)
	if len(theJob.IDs) > 0 {
		go func() {
			for _, id := range theJob.IDs {
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
	req, _ := http.NewRequest("GET", theJob.URL.String()+"__ids", nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		for i := 0; i < theJob.Count; i++ {
			select {
			case failures <- failure{conceptId: "", error: fmt.Errorf("Could not get /__ids from: %v (%v)", theJob.URL, err)}:
			default:
			}
		}
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		for i := 0; i < theJob.Count; i++ {
			select {
			case failures <- failure{
				conceptId: "",
				error: fmt.Errorf("Could not get /__ids from %v. Returned %v", theJob.URL, resp.StatusCode),
			}:
			default:
			}
		}
	}
	type listEntry struct {
		ID string `json:"id"`
	}
	var le listEntry
	dec := json.NewDecoder(resp.Body)
	for {
		err = dec.Decode(&le)
		if err != nil {
			if err == io.EOF {
				break
			}
			select {
			case failures <- failure{
				conceptId: "",
				error: fmt.Errorf("Error parsing one concept id from /__ids response. url=%v %v", theJob.URL, err),
			}:
			default:
			}
		}
		ids <- le.ID
	}
	close(ids)
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
			select {
			case failures <- failure{
				conceptId: id,
				error: fmt.Errorf("Could not get concept with uuid: %v (%v)", id, err),
			}:
			default:
			}
		}
		if resp.StatusCode != http.StatusOK {
			select {
			case failures <- failure{
				conceptId: id,
				error: fmt.Errorf("Could not get concept %v from %v. Returned %v", id, theJob.URL, resp.StatusCode),
			}:
			default:
			}
		}
		data, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			select {
			case failures <- failure{
				conceptId: id,
				error: fmt.Errorf("Could not get concept with uuid: %v (%v)", id, err),
			}:
			default:
				return
			}
		}
		concepts <- concept{id: id, payload: string(data)}
	}
}

func (s *publishService) getCount(ids []string, baseURL *url.URL, authorization string) (int, error) {
	if len(ids) > 0 {
		return len(ids), nil
	}
	req, _ := http.NewRequest("GET", baseURL.String()+"__count", nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return -1, fmt.Errorf("Could not connect to %v. Error (%v)", baseURL, err)
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
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

func (s *publishService) reload(baseURL *url.URL, authorization string) error {
	req, _ := http.NewRequest("POST", baseURL.String()+"__reload", nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Could not connect to url=%v to reload concepts: %v", baseURL, err)
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status=%d when reloading concepts at url=%v", resp.StatusCode, baseURL)
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
