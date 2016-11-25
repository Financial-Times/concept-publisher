package main

import (
	"testing"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"errors"
)

func TestGetJobIds_Empty(t *testing.T) {
	url, err := url.Parse("http://localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	var mockQueueSer queueServiceI = mockedQueueService{}
	var mockHttpSer httpServiceI = nilHttpService{}
	pubService := newPublishService(url, &mockQueueSer, &mockHttpSer)
	actualIds := pubService.getJobIds()
	expectedIds := []string{}
	if !reflect.DeepEqual(actualIds, expectedIds) {
		t.Errorf("wrong ids list: got %v want %v", actualIds, expectedIds)
	}
}

func TestCreateJob(t *testing.T) {
	tests := []struct {
		clusterUrl     string
		baseUrl        string
		conceptType    string
		ids            []string
		throttle       int
		createErr      error
		idToTID        map[string]string
		finalBaseUrl   string
	}{
		{
			clusterUrl: "http://localhost:8080",
			baseUrl: "http://localhost:8080/__special-reports-transformer/transformers/special-reports/",
			conceptType: "special-reports",
			ids: []string{},
			throttle: 1,
			createErr: nil,
			idToTID: make(map[string]string),
			finalBaseUrl: "http://localhost:8080/__special-reports-transformer/transformers/special-reports/",
		},
		{
			clusterUrl: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080",
			baseUrl: "http://somethingelse:8080/__special-reports-transformer/transformers/special-reports",
			conceptType: "special-reports",
			ids: []string{},
			throttle: 1,
			createErr: errors.New(`message="Can't find concept type in URL. Must be like the following __special-reports-transformer/transformers/special-reports/`),
			idToTID: make(map[string]string),
			finalBaseUrl: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__special-reports-transformer/transformers/topics/",
		},
		{
			clusterUrl: "http://localhost:8080",
			baseUrl: "/__special-reports-transformer/transformers/topics/",
			conceptType: "topics",
			ids: []string{},
			throttle: 1,
			createErr: nil,
			idToTID: make(map[string]string),
			finalBaseUrl: "http://localhost:8080/__special-reports-transformer/transformers/topics/",
		},
		{
			clusterUrl: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080",
			baseUrl: "/__topics-transformer/transformers/topics/",
			conceptType: "topics",
			ids: []string{"1", "2"},
			throttle: 1,
			createErr: nil,
			idToTID: map[string]string{
				"1": "",
				"2": "",
			},
			finalBaseUrl: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics/",
		},
	}
	for _, test := range tests {
		clusterUrl, err := url.Parse(test.clusterUrl)
		if err != nil {
			t.Error(err)
		}
		var mockQueueSer queueServiceI = mockedQueueService{}
		var mockHttpSer httpServiceI = nilHttpService{}
		pubService := newPublishService(clusterUrl, &mockQueueSer, &mockHttpSer)
		baseUrl, err := url.Parse(test.baseUrl)
		if err != nil {
			t.Error(err)
		}
		finalBaseUrl, err := url.Parse(test.finalBaseUrl)
		if err != nil {
			t.Error(err)
		}

		actualJob, err := pubService.createJob(test.ids, *baseUrl, test.throttle)

		if (err != nil) {
			if (test.createErr != nil) {
				if !strings.HasPrefix(err.Error(), test.createErr.Error()) {
					t.Fatalf("unexpected error. diff got vs want:\n%v\n%v", err, test.createErr)
				}
				continue
			}
			t.Errorf("unexpected error: %v", err)
			return
		}
		expectedJob := job{
			JobID: actualJob.JobID,
			ConceptType: test.conceptType,
			IDToTID: test.idToTID,
			URL: *finalBaseUrl,
			Throttle: test.throttle,
			Progress: 0,
			Status: defined,
			FailedIDs: []string{},
		}
		if !reflect.DeepEqual(*actualJob, expectedJob) {
			t.Errorf("wrong job. diff got vs want:\n%v\n%v", *actualJob, expectedJob)
		}
	}
}

func TestDeleteJob(t *testing.T) {
	tests := []struct {
		jobIDToDelete  string
		jobs           map[string]*job
		nJobsAfter     int
		deleteErr      error
	}{
		{
			jobIDToDelete: "job_1",
			jobs: map[string]*job{
				"job_1": &job{
					JobID: "job_1",
					ConceptType: "special-reports",
					IDToTID: make(map[string]string),
					Throttle: 1,
					Status: completed,
					FailedIDs: []string{},
				},
			},
			nJobsAfter: 0,
			deleteErr: nil,
		},
		{
			jobIDToDelete: "job_1",
			jobs: map[string]*job{
				"job_1": &job{
					JobID: "job_1",
					ConceptType: "special-reports",
					IDToTID: make(map[string]string),
					Throttle: 1,
					Status: inProgress,
					FailedIDs: []string{},
				},
			},
			nJobsAfter: 0,
			deleteErr: errors.New(`message="Job is in progress, locked."`),
		},
		{
			jobIDToDelete: "job_99",
			jobs: map[string]*job{},
			nJobsAfter: 0,
			deleteErr: errors.New(`message="Job not found"`),
		},
	}
	for _, test := range tests {
		clusterUrl, err := url.Parse("http://localhost:8080/")
		if err != nil {
			t.Fatal(err)
		}
		var mockQueueSer queueServiceI = mockedQueueService{}
		var mockHttpSer httpServiceI = nilHttpService{}
		pubService := publishService{
			clusterRouterAddress: clusterUrl,
			queueServiceI: &mockQueueSer,
			mutex: &sync.RWMutex{},
			jobs: test.jobs,
			httpService: &mockHttpSer,
		}

		err = pubService.deleteJob(test.jobIDToDelete)

		if (err != nil) {
			if (test.deleteErr != nil) {
				if !strings.HasPrefix(err.Error(), test.deleteErr.Error()) {
					t.Fatalf("unexpected error. diff got vs want:\n%v\n%v", err, test.deleteErr)
				}
				continue
			}
			t.Fatalf("unexpected error: %v", err)
			return
		}
		if len(pubService.jobs) != test.nJobsAfter {
			t.Fatalf("wrong number of jobs. diff got vs want:\n%d\n%d", len(pubService.jobs), test.nJobsAfter)
		}
	}
}

func TestRunJob(t *testing.T) {
	tests := []struct {
		baseURL    string
		httpSer    httpServiceI
		queueSer   queueServiceI
		idToTID    map[string]string
	}{
		{
			"http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics",
			mockedHttpService{
				reloadF: func(url string, authorization string) error {
					return nil
				},
				getIdsF: func(string, string) ([]byte, *failure) {
					return []byte(`{"id":"1"}
					{"id":"2"}`), nil
				},
				getCountF: func(string, string) (int, error) {
					return 2, nil
				},
				fetchConceptF: func(id string, url string, auth string) ([]byte, *failure) {
					if reflect.DeepEqual(id, "1") {
						return []byte(`{"uuid": "1"}`), nil
					}
					if reflect.DeepEqual(id, "2") {
						return []byte(`{"uuid": "2"}`), nil
					}
					return []byte{}, &failure{
						conceptID: id,
						error: errors.New("Requested something that shouldn't have."),
					}
				},
			},
			checkingQueueService{
				idsSent: []string{},
			},
			map[string]string{},
		},
	}
	for _, test := range tests {
		clusterUrl, err := url.Parse("http://ip-172-24-158-162.eu-west-1.compute.internal:8080")
		if err != nil {
			t.Fatal(err)
		}
		var mockQueueSer queueServiceI = mockedQueueService{}
		var mockHttpSer httpServiceI = test.httpSer
		if err != nil {
			t.Fatalf("unexpected error. %v", err)
		}
		testBaseUrl, err := url.Parse(test.baseURL)
		if err != nil {
			t.Fatalf("unexpected error. %v", err)
		}
		oneJob := &job{
			JobID: "job_1",
			URL: *testBaseUrl,
			ConceptType: "topics",
			IDToTID: test.idToTID,
			Throttle: 0,
			Status: defined,
			FailedIDs: []string{},
		}

		pubService := publishService{
			clusterRouterAddress: clusterUrl,
			queueServiceI: &mockQueueSer,
			mutex: &sync.RWMutex{},
			jobs: map[string]*job{
				"job_1": oneJob,
			},
			httpService: &mockHttpSer,
		}

		pubService.runJob(oneJob, "Basic 1234")

		_, ok := oneJob.IDToTID["1"]
		if !ok {
			t.Fatal("id 1 didn't publish")
		}
		_, ok = oneJob.IDToTID["2"]
		if !ok {
			t.Fatal("id 2 didn't publish")
		}
	}
}

type mockedQueueService struct {}

func (q mockedQueueService) sendMessage(id string, conceptType string, tid string, payload []byte) error {
	return nil
}

type checkingQueueService struct {
	idsSent   []string
}

func (q checkingQueueService) sendMessage(id string, conceptType string, tid string, payload []byte) error {
	q.idsSent = append(q.idsSent, id)
	return nil
}

type mockedHttpService struct {
	reloadF       func(string, string) error
	getIdsF       func(string, string) ([]byte, *failure)
	getCountF     func(string, string) (int, error)
	fetchConceptF func(string, string, string) ([]byte, *failure)
}

func (h mockedHttpService) reload(url string, authorization string) error {
	return h.reloadF(url, authorization)
}

func (h mockedHttpService) getIds(url string, authorization string) ([]byte, *failure) {
	return h.getIdsF(url, authorization)
}

func (h mockedHttpService) getCount(url string, authorization string) (int, error) {
	return h.getCountF(url, authorization)
}

func (h mockedHttpService) fetchConcept(conceptID string, url string, authorization string) ([]byte, *failure) {
	return h.fetchConceptF(conceptID, url, authorization)
}

type nilHttpService struct {}

func (h nilHttpService) reload(url string, authorization string) error {
	return nil
}

func (h nilHttpService) getIds(url string, authorization string) ([]byte, *failure) {
	return []byte{}, nil
}

func (h nilHttpService) getCount(url string, authorization string) (int, error) {
	return -1, nil
}

func (h nilHttpService) fetchConcept(conceptID string, url string, authorization string) ([]byte, *failure) {
	return []byte{}, nil
}