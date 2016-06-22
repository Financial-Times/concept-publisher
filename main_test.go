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
)

const (
	CreateJob                = "{\"concept\":\"organisations\",\"url\": \"http://localhost:8080/transformers/organisations/\", \"throttle\": 100}"
	CreateJobInvalidPayload  = "\"concept\":\"organisations\",\"url\": \"http://localhost:8080/transformers/organisations/\", \"throttle\": 100}"
	CreateJobMissingUrl      = "{\"concept\":\"organisations\", \"throttle\": 100}"
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
		{"Success - create new job", newRequest("POST", "/jobs", CreateJob), http.StatusOK, "application/json", "{\"jobId\":\"job_id\"}\n"},
		{"Bad request - create new job missing concept", newRequest("POST", "/jobs", CreateJobMissingConcept), http.StatusBadRequest, "text/plain; charset=utf-8", "Concept empty\n"},
		{"Bad request - create new job invalid throttle", newRequest("POST", "/jobs", CreateJobInvalidThrottle), http.StatusBadRequest, "text/plain; charset=utf-8", "Invalid throttle: -200\n"},
		{"Bad request - create new job missing url", newRequest("POST", "/jobs", CreateJobMissingUrl), http.StatusBadRequest, "text/plain; charset=utf-8", "Base url empty\n"},
		{"Bad request - create new job invalid payload", newRequest("POST", "/jobs", CreateJobInvalidPayload), http.StatusBadRequest, "text/plain; charset=utf-8", "Invalid payload: (json: cannot unmarshal string into Go value of type main.createJobRequest)\n"},
		{"Success - get job", newRequest("GET", "/jobs/jobID", ""), http.StatusOK, "application/json", "{\"concept\":\"organisations\",\"url\":\"http://localhost:8080/transformers/organisations/\",\"throttle\":100,\"count\":1000,\"done\":150,\"status\":\"In progress\"}\n"},
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

func (d *dummyService) newJob(concept string, baseURL *url.URL, authorization string, throttle int) string {
	return "job_id"
}
func (d *dummyService) jobStatus(jobID string) (*jobStatus, error) {
	return &jobStatus{Concept: "organisations", URL: "http://localhost:8080/transformers/organisations/", Throttle: 100, Status: "In progress", Done: 150, Count: 1000}, nil
}
func (d *dummyService) getJobs() []job {
	return []job{job{JobID: "job_id"}}
}
