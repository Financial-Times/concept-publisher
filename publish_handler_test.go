package main

import (
	"testing"
	"net/http/httptest"
	"net/http"
	"net/url"
	"strings"
)

func TestGetJobIds_Empty(t *testing.T) {
	req, err := http.NewRequest("GET", "/jobs", nil)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	url, err := url.Parse("http://localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	pubService := newPublishService(url, nil, nil)
	pubHandler := newPublishHandler(&pubService)
	handler := http.HandlerFunc(pubHandler.listJobs)
	handler.ServeHTTP(recorder, req)
	status := recorder.Code;
	if status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}
	expected := `[]`
	actual := strings.TrimSpace(recorder.Body.String())
	if actual != expected {
		t.Errorf("handler returned unexpected body: got '%v' want '%v'", actual, expected)
	}
}
