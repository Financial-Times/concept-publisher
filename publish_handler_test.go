package main

import (
	"testing"
	"net/http"
	"net/http/httptest"
	"net/url"
	"github.com/golang/go/src/pkg/bytes"
	"github.com/pkg/errors"
)

func TestHandlerCreateJob(t *testing.T) {
	tests := []struct {
		name           string
		httpBody       string
		createJobF     func (ids []string, baseURL url.URL, throttle int) (*job, error)
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
			name:           "missing fields",
			httpBody:       `{"url":"/__topics-transformer/transformers/topics"}`,
			createJobF:     func (ids []string, baseURL url.URL, throttle int) (*job, error) {
				return &job{JobID: "1"}, nil
			},
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "error at subsequent call",
			httpBody:       `{"url":"/__topics-transformer/transformers/topics", throttle: 100}`,
			createJobF:     func (ids []string, baseURL url.URL, throttle int) (*job, error) {
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
		var pubService publishServiceI = mockedPublishService{
			createJobF: test.createJobF,
			runJobF: func (theJob *job, authorization string) {},
		}
		pubHandler := newPublishHandler(&pubService)
		recorder := httptest.NewRecorder()
		handler := http.HandlerFunc(pubHandler.createJob)

		handler.ServeHTTP(recorder, request)
		
		if actualStatus := recorder.Code; actualStatus != test.expectedStatus {
			t.Errorf("%s\nhandler returned wrong status code: got %v want %v", test.name, actualStatus, test.expectedStatus)
		}
	}
}

type mockedPublishService struct {
	createJobF func (ids []string, baseURL url.URL, throttle int) (*job, error)
	getJobF    func (jobID string) (*job, error)
	getJobIdsF func () []string
	runJobF    func (theJob *job, authorization string)
	deleteJobF func (jobID string) error
}

func (p mockedPublishService) createJob(ids []string, baseURL url.URL, throttle int) (*job, error) {
	return p.createJobF(ids, baseURL, throttle)
}

func (p mockedPublishService) getJob(jobID string) (*job, error) {
	return p.getJobF(jobID)
}

func (p mockedPublishService) getJobIds() []string {
	return p.getJobIdsF()
}

func (p mockedPublishService) runJob(theJob *job, authorization string) {
	p.runJobF(theJob, authorization)
}

func (p mockedPublishService) deleteJob(jobID string) error {
	return p.deleteJobF(jobID)
}
