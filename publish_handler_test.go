package main

//import (
//	"testing"
//	"net/http/httptest"
//	"net/http"
//	"net/url"
//	"strings"
//	"github.com/golang/go/src/pkg/bytes"
//)
//
//func TestListJobs_Empty(t *testing.T) {
//	req, err := http.NewRequest("GET", "/jobs", nil)
//	if err != nil {
//		t.Fatal(err)
//	}
//	recorder := httptest.NewRecorder()
//	url, err := url.Parse("http://localhost:8080")
//	if err != nil {
//		t.Fatal(err)
//	}
//	var pubService publishServiceI = newPublishService(url, nil, nil)
//	var pubHandler publishHandler = newPublishHandler(&pubService)
//	handler := http.HandlerFunc(pubHandler.listJobs)
//	handler.ServeHTTP(recorder, req)
//	expectedStatus := http.StatusOK
//	actualStatus := recorder.Code;
//	if actualStatus != expectedStatus {
//		t.Errorf("handler returned wrong status code: got %v want %v", actualStatus, expectedStatus)
//	}
//	expectedBody := `[]`
//	actualBody := strings.TrimSpace(recorder.Body.String())
//	if actualBody != expectedBody {
//		t.Errorf("handler returned unexpected body: got '%v' want '%v'", actualBody, expectedBody)
//	}
//}
//
//func TestCreateJob(t *testing.T) {
//	tests := []struct {
//		inputPayload   string
//		expectedStatus int
//	}{
//		{
//			inputPayload:   `{"url": "__special-reports-transformer/transformers/special-reports/", "throttle": 100, "authorization": "Basic abcd"}`,
//			expectedStatus: http.StatusCreated,
//		},
//		{
//			inputPayload:   `{"url": "__special-reports-transformer/transformers/special-reports", "throttle": 100, "authorization": "Basic abcd"}`,
//			expectedStatus: http.StatusBadRequest,
//		},
//		{
//			inputPayload:   `{"url": "__special-reports-transformer/transformers/special-reports/", "throttle": 100}`,
//			expectedStatus: http.StatusCreated,
//		},
//		{
//			inputPayload:   `{"url": "__special-reports-transformer/transformers/special-reports/"}`,
//			expectedStatus: http.StatusBadRequest,
//		},
//	}
//	for _, test := range tests {
//		bodyReader := bytes.NewReader([]byte(test.inputPayload))
//		req, err := http.NewRequest("POST", "/jobs", bodyReader)
//		if err != nil {
//			t.Fatal(err)
//		}
//		req.Header.Add("Content-Type", "application/json")
//		recorder := httptest.NewRecorder()
//		url, err := url.Parse("http://localhost:8080")
//		if err != nil {
//			t.Fatal(err)
//		}
//		var pubService publishServiceI = newPublishService(url, nil, nil)
//		pubHandler := newPublishHandler(&pubService)
//		handler := http.HandlerFunc(pubHandler.createJob)
//		handler.ServeHTTP(recorder, req)
//		actualStatus := recorder.Code
//		if actualStatus != test.expectedStatus {
//			t.Errorf("handler returned wrong status code: got %v want %v", actualStatus, test.expectedStatus)
//		}
//	}
//}
