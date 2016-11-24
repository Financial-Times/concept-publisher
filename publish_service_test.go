package main

import (
	"testing"
	"net/url"
	"reflect"
)

func TestGetJobIds_Empty(t *testing.T) {
	url, err := url.Parse("http://localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	var mockQueueSer queueServiceI = mockedQueueService{}
	var mockHttpSer httpServiceI = mockedHttpService{}
	pubService := newPublishService(url, &mockQueueSer, &mockHttpSer)
	actualIds := pubService.getJobIds()
	expectedIds := []string{}
	if !reflect.DeepEqual(actualIds, expectedIds) {
		t.Errorf("wrong ids list: got %v want %v", actualIds, expectedIds)
	}
}

type mockedQueueService struct {
}

func (q mockedQueueService) sendMessage(id string, conceptType string, tid string, payload []byte) error {
	return nil
}

type mockedHttpService struct {
}

func (h mockedHttpService) reload(url string, authorization string) error {
	return nil
}

func (h mockedHttpService) getIds(url string, authorization string) ([]byte, *failure) {
	return []byte{}, nil
}

func (h mockedHttpService) getCount(url string, authorization string) (int, error) {
	return -1, nil
}

func (h mockedHttpService) fetchConcept(conceptID string, url string, authorization string) ([]byte, *failure) {
	return []byte{}, nil
}