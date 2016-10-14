package main

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"errors"
)

const (
	CreateJob                = "{\"concept\":\"organisations\",\"url\": \"http://localhost:8080/transformers/organisations/\", \"throttle\": 100}"
	CreateJobWithIDS         = "{\"concept\":\"organisations\",\"url\": \"http://localhost:8080/transformers/organisations/\", \"throttle\": 100, \"ids\":[\"uuids1\", \"uuids2\"]}"
	CreateJobInvalidPayload  = "\"concept\":\"organisations\",\"url\": \"http://localhost:8080/transformers/organisations/\", \"throttle\": 100}"
	CreateJobMissingURL      = "{\"concept\":\"organisations\", \"throttle\": 100}"
	CreateJobInvalidThrottle = "{\"concept\":\"organisations\",\"url\": \"http://localhost:8080/transformers/organisations/\", \"throttle\": -200}"
	CreateJobMissingConcept  = "{\"url\": \"http://localhost:8080/transformers/organisations/\", \"throttle\": 100}"
)

func TestHandlers(t *testing.T) {
	assert := assert.New(t)
	tests := []struct {
		name        string
		req         *http.Request
		statusCode  int
		contentType string // Contents of the Content-Type header
		body        string
	}{
		{"Success - create new job without ids", newRequest("POST", "/jobs", CreateJob), http.StatusOK, "application/json", "{\"jobId\":\"job_id\"}\n"},
		{"Success - create new job with ids", newRequest("POST", "/jobs", CreateJobWithIDS), http.StatusOK, "application/json", "{\"jobId\":\"job_id\"}\n"},
		{"Bad request - create new job missing concept", newRequest("POST", "/jobs", CreateJobMissingConcept), http.StatusBadRequest, "text/plain; charset=utf-8", "Concept empty\n"},
		{"Bad request - create new job invalid throttle", newRequest("POST", "/jobs", CreateJobInvalidThrottle), http.StatusBadRequest, "text/plain; charset=utf-8", "Invalid throttle: -200\n"},
		{"Bad request - create new job missing url", newRequest("POST", "/jobs", CreateJobMissingURL), http.StatusBadRequest, "text/plain; charset=utf-8", "Base url empty\n"},
		{"Bad request - create new job invalid payload", newRequest("POST", "/jobs", CreateJobInvalidPayload), http.StatusBadRequest, "text/plain; charset=utf-8", "Invalid payload: (json: cannot unmarshal string into Go value of type main.createJobRequest)\n"},
		{"Success - get job", newRequest("GET", "/jobs/jobID", ""), http.StatusOK, "application/json", "{\"concept\":\"organisations\",\"url\":\"http://localhost:8080/transformers/organisations/\",\"throttle\":100,\"count\":1000,\"done\":150,\"status\":\"In progress\"}\n"},
		{"Success - get job with ids", newRequest("GET", "/jobs/jobIDWithIDS", ""), http.StatusOK, "application/json", "{\"concept\":\"organisations\",\"ids\":[\"uuid1\",\"uuid2\"],\"url\":\"http://localhost:8080/transformers/organisations/\",\"throttle\":100,\"count\":1000,\"done\":150,\"status\":\"In progress\"}\n"},
		{"Bad request - get job Does Not Exist", newRequest("GET", "/jobs/jobIDDoesNotExist", ""), http.StatusNotFound, "text/plain; charset=utf-8", "Job with id jobIDDoesNotExist does not exist\n"},
		{"Success - get list jobs", newRequest("GET", "/jobs", ""), http.StatusOK, "application/json", "[{\"jobId\":\"job_id\"}]\n"},
	}

	for _, test := range tests {
		rec := httptest.NewRecorder()
		router(&dummyService{}).ServeHTTP(rec, test.req)
		assert.True(test.statusCode == rec.Code, fmt.Sprintf("%s: Wrong response code, was %d, should be %d", test.name, rec.Code, test.statusCode))
		assert.True(test.contentType == rec.Header().Get("Content-Type"), fmt.Sprintf("%s: Wrong Content-Type, was %s, should be %s", test.name, rec.Header().Get("Content-Type"), test.contentType))
		assert.Equal(test.body, rec.Body.String(), fmt.Sprintf("%s: Wrong body", test.name))
	}
}

func newRequest(method, url string, body string) *http.Request {
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	return req
}

func router(s pubService) *mux.Router {
	m := mux.NewRouter()
	h := &handler{service: s, health: health{}}
	m.HandleFunc("/jobs", h.createJob).Methods("POST")
	m.HandleFunc("/jobs", h.listJobs).Methods("GET")
	m.HandleFunc("/jobs/{id}", h.status).Methods("GET")
	return m
}

type dummyService struct {
}

func (d *dummyService) newJob(concept string, ids []string, baseURL *url.URL, authorization string, throttle int) string {
	return "job_id"
}
func (d *dummyService) jobStatus(jobID string) (jobStatus, error) {
	if "jobID" == jobID {
		return jobStatus{Concept: "organisations", URL: "http://localhost:8080/transformers/organisations/", Throttle: 100, Status: "In progress", Done: 150, Count: 1000}, nil
	}
	if "jobIDWithIDS" == jobID {
		return jobStatus{Concept: "organisations", URL: "http://localhost:8080/transformers/organisations/", IDS: []string{"uuid1", "uuid2"}, Throttle: 100, Status: "In progress", Done: 150, Count: 1000}, nil
	}
	if "jobIDDoesNotExist" == jobID {
		return jobStatus{}, errors.New("Job with id " + jobID + " does not exist")

	}
	return jobStatus{}, fmt.Errorf("Invalid id %v", jobID)
}
func (d *dummyService) getJobs() []job {
	return []job{job{JobID: "job_id"}}
}
