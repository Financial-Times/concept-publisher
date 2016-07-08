package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
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

var httpClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 128,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	},
}

type pubService interface {
	newJob(concept string, ids []string, baseURL *url.URL, authorization string, throttle int) string
	jobStatus(jobID string) (jobStatus, error)
	getJobs() []job
}

type publishService struct {
	transAddr *url.URL
	producer  producer.MessageProducer
	mutex     *sync.RWMutex //protects jobs
	jobs      map[string]*jobStatus
}

func newPublishService(transAddr *url.URL, producer producer.MessageProducer) publishService {
	return publishService{transAddr: transAddr, producer: producer, mutex: &sync.RWMutex{}, jobs: make(map[string]*jobStatus)}
}

func (s *publishService) newJob(concept string, ids []string, baseURL *url.URL, authorization string, throttle int) string {
	if baseURL.Host == "" {
		baseURL.Scheme = s.transAddr.Scheme
		baseURL.Host = s.transAddr.Host
	}
	jobID := "job_" + generateID()
	c, err := getCount(ids, baseURL, authorization)
	if err != nil {
		log.Errorf("Could not determine count: (%v)", err)
		s.mutex.Lock()
		s.jobs[jobID] = &jobStatus{Concept: concept, IDS: ids, URL: baseURL.String(), Throttle: throttle, Count: c, Done: 0, Status: "Failed"}
		s.mutex.Unlock()
		return jobID
	}
	s.mutex.Lock()
	s.jobs[jobID] = &jobStatus{Concept: concept, IDS: ids, URL: baseURL.String(), Throttle: throttle, Count: c, Done: 0, Status: "In progress"}
	s.mutex.Unlock()
	go s.publishConcepts(jobID, concept, ids, baseURL, authorization, throttle)

	return jobID
}

func (s *publishService) jobStatus(jobID string) (jobStatus, error) {
	s.mutex.RLock()
	job := *s.jobs[jobID]
	s.mutex.RUnlock()

	return job, nil
}

func (s *publishService) getJobs() []job {
	var jobs []job
	s.mutex.RLock()
	for k := range s.jobs {
		jobs = append(jobs, job{JobID: k})
	}
	s.mutex.RUnlock()

	if jobs == nil {
		return []job{} // ensure we return an empty array instead of null
	}

	return jobs
}

type concept struct {
	id      string
	payload string
}

func (s *publishService) publishConcepts(jobID string, conceptType string, ids []string, baseURL *url.URL, authorization string, throttle int) {
	ticker := time.NewTicker(time.Second / time.Duration(throttle))

	concepts := make(chan concept, 128)
	errors := make(chan error, 1)

	go func() {
		fetchAll(ids, baseURL, authorization, concepts, errors, ticker)
		close(concepts)
	}()
	s.mutex.RLock()
	count := s.jobs[jobID].Count
	s.mutex.RUnlock()
	th := 1000
	for i := 0; i < count; i++ {
		select {
		case err := <-errors:
			s.handleErr(jobID, err)
			return
		case c := <-concepts:
			message := producer.Message{Headers: buildHeader(c.id, conceptType), Body: c.payload}
			err := s.producer.SendMessage(c.id, message)
			if err != nil {
				s.handleErr(jobID, err)
				return
			}
			if i%th == 0 {
				s.mutex.Lock()
				s.jobs[jobID].Done = i
				s.mutex.Unlock()
			}
		}
	}
	s.mutex.Lock()
	s.jobs[jobID].Status = "Completed"
	s.jobs[jobID].Done = count
	s.mutex.Unlock()
	return
}

func (s *publishService) handleErr(jobID string, err error) {
	log.Errorf("Error with job: %v (%v)", jobID, err)
	s.mutex.Lock()
	s.jobs[jobID].Status = "Failed"
	s.mutex.Unlock()
}

func fetchAll(ids []string, baseURL *url.URL, authorization string, concepts chan<- concept, errors chan<- error, ticker *time.Ticker) {
	idsChan := make(chan string, 128)
	if len(ids) > 0 {
		go func() {
			for _, id := range ids {
				idsChan <- id
			}
			close(idsChan)
		}()

	} else {
		go fetchIDList(baseURL, authorization, idsChan, errors)
	}

	readers := 100

	readWg := sync.WaitGroup{}

	for i := 0; i < readers; i++ {
		readWg.Add(1)
		go func(i int) {
			fetchConcepts(baseURL, authorization, concepts, idsChan, errors, ticker)
			readWg.Done()
		}(i)
	}

	readWg.Wait()
}

func fetchIDList(baseURL *url.URL, authorization string, ids chan<- string, errors chan<- error) {
	req, _ := http.NewRequest("GET", baseURL.String()+"__ids", nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := httpClient.Do(req)
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

func fetchConcepts(baseURL *url.URL, authorization string, concepts chan<- concept, ids <-chan string, errors chan<- error, ticker *time.Ticker) {
	for id := range ids {
		<-ticker.C
		req, _ := http.NewRequest("GET", baseURL.String()+id, nil)
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		resp, err := httpClient.Do(req)
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

func getCount(ids []string, baseURL *url.URL, authorization string) (int, error) {
	if len(ids) > 0 {
		return len(ids), nil
	}
	req, _ := http.NewRequest("GET", baseURL.String()+"__count", nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := httpClient.Do(req)
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

func buildHeader(uuid string, concept string) map[string]string {
	return map[string]string{
		"Message-Id":        uuid,
		"Message-Type":      concept,
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
