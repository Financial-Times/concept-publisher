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

type job struct {
	JobID       string   `json:"jobId"`
	ConceptType string   `json:"concept"`
	IDS         []string `json:"ids,omitempty"`
	URL         url.URL  `json:"url"`
	Throttle    int      `json:"throttle"`
	Count       int      `json:"count"`
	Progress    int      `json:"done"`
	Status      string   `json:"status"`
}

func (j job) String() string {
	return fmt.Sprintf("Concept=%s URL=%s Count=%d Throttle=%d Status=%s", j.ConceptType, j.URL, j.Count, j.Throttle, j.Status)
}

type NotFoundError struct {
	msg string
}

func (e *NotFoundError) Error() string {
	return e.msg
}

func newNotFoundError(jobID string) NotFoundError {
	return NotFoundError{msg: fmt.Sprintf("Job with id=%s not found", jobID)}
}

type concept struct {
	id      string
	payload string
}

type pubService interface {
	newJob(concept string, ids []string, baseURL *url.URL, authorization string, throttle int) (*job, error)
	getJob(jobID string) (*job, NotFoundError)
	getJobStatus(jobID string) (string, NotFoundError)
	getJobs() []string
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

func (s *publishService) newJob(conceptType string, ids []string, baseURL *url.URL, authorization string, throttle int) (job, error) {
	if baseURL.Host == "" {
		baseURL.Scheme = s.clusterRouterAddress.Scheme
		baseURL.Host = s.clusterRouterAddress.Host
	}
	err := s.reload(baseURL, authorization)
	if err != nil {
		log.InfoLevel("Couldn't reload concept=%s %v", conceptType, err)
	}
	count, err := s.getCount(ids, baseURL, authorization)
	if err != nil {
		return nil, fmt.Errorf("Could not determine count for concept=%s %v", conceptType, err)
	}
	s.mutex.Lock()
	jobID := "job_" + generateID()
	theJob := &job{
		ConceptType: conceptType,
		IDS: ids,
		URL: baseURL,
		Throttle: throttle,
		Count: count,
		Progress: 0,
		Status: "In progress",
	}
	s.jobs[jobID] = theJob
	s.mutex.Unlock()
	log.Infof("Created job with jobID=%s", theJob.JobID)
	go s.runJob(theJob, authorization)
	return theJob
}

func (s *publishService) getJob(jobID string) (*job, NotFoundError) {
	s.mutex.RLock()
	job, ok := s.jobs[jobID]
	if !ok {
		return nil, newNotFoundError(jobID)
	}
	s.mutex.RUnlock()
	return job, nil
}

func (s *publishService) getJobStatus(jobID string) (string, NotFoundError) {
	job, err := s.getJob(jobID)
	if err != nil {
		return nil, err
	}
	return job.Status, nil
}

func (s *publishService) getJobs() []string {
	jobIds := []string{}
	s.mutex.RLock()
	for _, j := range s.jobs {
		jobIds = append(jobIds, j.JobID)
	}
	s.mutex.RUnlock()
	return jobIds
}

func (s *publishService) runJob(theJob *job, authorization string) {
	ticker := time.NewTicker(time.Second / time.Duration(job.Throttle))
	concepts := make(chan concept, 128)
	errors := make(chan error, 1)
	go func() {
		s.fetchAll(job.IDS, job.URL, authorization, concepts, errors, ticker)
		close(concepts)
	}()
	count := theJob.Count
	for job.Progress = 0; job.Progress < count; job.Progress++ {
		select {
		case err := <-errors:
			s.handleErr(theJob, err)
			return
		case c := <-concepts:
			message := producer.Message{
				Headers: buildHeader(c.id, job.ConceptType),
				Body: c.payload,
			}
			err := s.producer.SendMessage(c.id, message)
			if err != nil {
				s.handleErr(theJob, err)
				return
			}
		}
	}
	job.Status = "Completed"
	log.Infof("Completed job with jobID=%s", theJob.JobID)
	return
}

func (s *publishService) handleErr(theJob *job, err error) {
	log.Errorf("Error with jobID=%v %v", job.JobID, err)
	theJob.Status = "Failed"
}

func (s *publishService) fetchAll(ids []string, baseURL *url.URL, authorization string, concepts chan<- concept, errors chan<- error, ticker *time.Ticker) {
	idsChan := make(chan string, 128)
	if len(ids) > 0 {
		go func() {
			for _, id := range ids {
				idsChan <- id
			}
			close(idsChan)
		}()

	} else {
		go s.fetchIDList(baseURL, authorization, idsChan, errors)
	}
	readers := 100
	readWg := sync.WaitGroup{}
	for i := 0; i < readers; i++ {
		readWg.Add(1)
		go func(i int) {
			s.fetchConcepts(baseURL, authorization, concepts, idsChan, errors, ticker)
			readWg.Done()
		}(i)
	}
	readWg.Wait()
}

func (s *publishService) fetchIDList(baseURL *url.URL, authorization string, ids chan<- string, errors chan<- error) {
	req, _ := http.NewRequest("GET", baseURL.String()+"__ids", nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		select {
		case errors <- fmt.Errorf("Could not get /__ids from: %v (%v)", baseURL, err):
		default:
			return
		}
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		select {
		case errors <- fmt.Errorf("Could not get /__ids from %v. Returned %v", baseURL, resp.StatusCode):
		default:
			return
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
			case errors <- fmt.Errorf("Error parsing /__ids response: %v (%v)", baseURL, err):
			default:
				return
			}
		}

		ids <- le.ID
	}
	close(ids)
}

func (s *publishService) fetchConcepts(baseURL *url.URL, authorization string, concepts chan<- concept, ids <-chan string, errors chan<- error, ticker *time.Ticker) {
	for id := range ids {
		<-ticker.C
		req, _ := http.NewRequest("GET", baseURL.String()+id, nil)
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		resp, err := s.httpClient.Do(req)
		if err != nil {
			select {
			case errors <- fmt.Errorf("Could not get concept with uuid: %v (%v)", id, err):
			default:
			}
			return
		}
		if resp.StatusCode != http.StatusOK {
			select {
			case errors <- fmt.Errorf("Could not get concept %v from %v. Returned %v", id, baseURL, resp.StatusCode):
			default:
				return
			}
		}
		data, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			select {
			case errors <- fmt.Errorf("Could not get concept with uuid: %v (%v)", id, err):
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
