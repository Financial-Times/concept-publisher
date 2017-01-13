package main

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestHandlerCreateJob(t *testing.T) {
	tests := []struct {
		name           string
		httpBody       string
		createJobF     func(ids []string, baseURL string, gtgURL string, throttle int) (*job, error)
		expectedStatus int
	}{
		{
			name:           "not a json",
			httpBody:       "not a json",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "illegal url",
			httpBody:       `{"url":""}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "illegal url",
			httpBody:       `{"url":"/__topics-transformer/transformers/topics"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "missing fields",
			httpBody:       `{"concept":"topics", "url":"/__topics-transformer/transformers/topics"}`,
			createJobF:     func(ids []string, baseURL string, gtgURL string, throttle int) (*job, error) {
				return &job{JobID: "1"}, nil
			},
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "error at subsequent call",
			httpBody:       `{"concept":"topics", "url":"/__topics-transformer/transformers/topics", "throttle": 100}`,
			createJobF:     func(ids []string, baseURL string, gtgURL string, throttle int) (*job, error) {
				return nil, errors.New("error creating job because of something")
			},
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, test := range tests {
		request, err := http.NewRequest("POST", "/jobs", bytes.NewReader([]byte(test.httpBody)))
		if err != nil {
			t.Error(err)
		}
		var pubService publisher = mockedPublisher{
			createJobF: test.createJobF,
			runJobF:    func(theJob *job, authorization string) {},
		}
		pubHandler := newPublishHandler(&pubService)
		recorder := httptest.NewRecorder()
		handler := http.HandlerFunc(pubHandler.createJob)

		handler.ServeHTTP(recorder, request)

		if actualStatus := recorder.Code; actualStatus != test.expectedStatus {
			t.Errorf("%s\nhandler returned wrong status code: got %v want %v", test.name, actualStatus, test.expectedStatus)
		}
		resp := string(recorder.Body.Bytes())
		if test.expectedStatus == http.StatusCreated && !strings.HasPrefix(resp, `{"jobID":`) {
			t.Errorf("%s\nhandler didn't return created job id: %v", test.name, resp)
		}
	}
}

func TestHandlerStatus(t *testing.T) {
	tests := []struct {
		name           string
		jobID          string
		getJobF        func(string) (*job, error)
		expectedStatus int
	}{
		{
			name:  "normal case",
			jobID: "1",
			getJobF: func(jobID string) (*job, error) {
				return &job{JobID: "1"}, nil
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:  "not found",
			jobID: "1",
			getJobF: func(jobID string) (*job, error) {
				return nil, newNotFoundError("1")
			},
			expectedStatus: http.StatusNotFound,
		},
	}
	for _, test := range tests {
		request, err := http.NewRequest("GET", "/jobs/"+test.jobID, nil)
		if err != nil {
			t.Error(err)
		}
		var pubService publisher = mockedPublisher{
			getJobF: test.getJobF,
		}
		pubHandler := newPublishHandler(&pubService)
		recorder := httptest.NewRecorder()
		handler := http.HandlerFunc(pubHandler.status)

		handler.ServeHTTP(recorder, request)

		if actualStatus := recorder.Code; actualStatus != test.expectedStatus {
			t.Errorf("%s\nhandler returned wrong status code: got %v want %v", test.name, actualStatus, test.expectedStatus)
		}
	}
}

func TestHandlerJobs(t *testing.T) {
	tests := []struct {
		name string
		getJobIdsF     func() []string
		expectedStatus int
		expectedBody   string
	}{
		{
			name: "normal case",
			//IDs:          []string {"1", "2"},
			getJobIdsF: func() []string {
				return []string{"1", "2"}
			},
			expectedStatus: http.StatusOK,
			expectedBody:   `["1","2"]`,
		},
	}
	for _, test := range tests {
		request, err := http.NewRequest("GET", "/jobs", nil)
		if err != nil {
			t.Error(err)
		}
		var pubService publisher = mockedPublisher{
			getJobIdsF: test.getJobIdsF,
		}
		pubHandler := newPublishHandler(&pubService)
		recorder := httptest.NewRecorder()
		handler := http.HandlerFunc(pubHandler.listJobs)

		handler.ServeHTTP(recorder, request)

		if actualStatus := recorder.Code; actualStatus != test.expectedStatus {
			t.Errorf("%s\nhandler returned wrong status code: got %v want %v", test.name, actualStatus, test.expectedStatus)
		}
		if !reflect.DeepEqual(strings.TrimSpace(string(recorder.Body.Bytes())), test.expectedBody) {
			t.Errorf("%s\nhandler returned wrong response body. got vs want:\n%s\n%s", test.name, string(recorder.Body.Bytes()), test.expectedBody)
		}
	}
}

func TestHandlerDeleteJob(t *testing.T) {
	tests := []struct {
		name           string
		jobID          string
		deleteJobF     func(string) error
		expectedStatus int
	}{
		{
			name:  "normal case",
			jobID: "1",
			deleteJobF: func(jobID string) error {
				return nil
			},
			expectedStatus: http.StatusNoContent,
		},
		{
			name:  "not found",
			jobID: "1",
			deleteJobF: func(jobID string) error {
				return newNotFoundError("1")
			},
			expectedStatus: http.StatusNotFound,
		},
		{
			name:  "unexpected error",
			jobID: "1",
			deleteJobF: func(jobID string) error {
				return errors.New("unexpected error")
			},
			expectedStatus: http.StatusInternalServerError,
		},
	}
	for _, test := range tests {
		request, err := http.NewRequest("DELETE", "/jobs/"+test.jobID, nil)
		if err != nil {
			t.Error(err)
		}
		var pubService publisher = mockedPublisher{
			deleteJobF: test.deleteJobF,
		}
		pubHandler := newPublishHandler(&pubService)
		recorder := httptest.NewRecorder()
		handler := http.HandlerFunc(pubHandler.deleteJob)

		handler.ServeHTTP(recorder, request)

		if actualStatus := recorder.Code; actualStatus != test.expectedStatus {
			t.Errorf("%s\nhandler returned wrong status code: got %v want %v", test.name, actualStatus, test.expectedStatus)
		}
	}
}

type mockedPublisher struct {
	createJobF func(ids []string, baseURL string, gtgURL string, throttle int) (*job, error)
	getJobF    func(jobID string) (*job, error)
	getJobIdsF func() []string
	runJobF    func(theJob *job, authorization string)
	deleteJobF func(jobID string) error
}

func (p mockedPublisher) createJob(conceptType string, ids []string, baseURL string, gtgURL string, throttle int) (*job, error) {
	return p.createJobF(ids, baseURL, baseURL+"__gtg", throttle)
}

func (p mockedPublisher) getJob(jobID string) (*job, error) {
	return p.getJobF(jobID)
}

func (p mockedPublisher) getJobIds() []string {
	return p.getJobIdsF()
}

func (p mockedPublisher) runJob(theJob *job, authorization string) {
	p.runJobF(theJob, authorization)
}

func (p mockedPublisher) deleteJob(jobID string) error {
	return p.deleteJobF(jobID)
}
