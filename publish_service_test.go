package main

import (
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestGetJobIds_Empty(t *testing.T) {
	url, err := url.Parse("http://localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	var mockQueueSer queue = allOkQueue{}
	var mockHttpSer caller = nilHttpService{}
	pubService := newPublishService(url, &mockQueueSer, &mockHttpSer)
	actualIds := pubService.getJobIds()
	expectedIds := []string{}
	if !reflect.DeepEqual(actualIds, expectedIds) {
		t.Errorf("wrong ids list: got %v want %v", actualIds, expectedIds)
	}
}

func TestGetJobIds_1(t *testing.T) {
	pubService := publishService{
		jobs: map[string]*job{
			"job_1": &job{
				JobID: "job_1",
			},
		},
	}
	actualIds := pubService.getJobIds()
	expectedIds := []string{"job_1"}
	if !reflect.DeepEqual(actualIds, expectedIds) {
		t.Errorf("wrong ids list: got %v want %v", actualIds, expectedIds)
	}
}

func TestCreateJob(t *testing.T) {
	tests := []struct {
		clusterUrl   string
		baseUrl      string
		conceptType  string
		ids          []string
		throttle     int
		createErr    error
		idToTID      map[string]string
		finalBaseUrl string
	}{
		{
			clusterUrl:   "http://localhost:8080",
			baseUrl:      "http://localhost:8080/__special-reports-transformer/transformers/special-reports/",
			conceptType:  "special-reports",
			ids:          []string{},
			throttle:     1,
			createErr:    nil,
			idToTID:      make(map[string]string),
			finalBaseUrl: "http://localhost:8080/__special-reports-transformer/transformers/special-reports/",
		},
		{
			clusterUrl:   "http://ip-172-24-158-162.eu-west-1.compute.internal:8080",
			baseUrl:      "/__special-reports-transformer/transformers/special-reports",
			conceptType:  "special-reports",
			ids:          []string{},
			throttle:     1,
			createErr:    errors.New(`message="Can't find concept type in URL. Must be like the following __special-reports-transformer/transformers/special-reports/`),
			idToTID:      make(map[string]string),
			finalBaseUrl: "http://somethingelse:9090/__special-reports-transformer/transformers/topics/",
		},
		{
			clusterUrl:   "http://ip-172-24-158-162.eu-west-1.compute.internal:8080",
			baseUrl:      "http://somethingelse:9090/__special-reports-transformer/transformers/special-reports/",
			conceptType:  "special-reports",
			ids:          []string{},
			throttle:     1,
			createErr:    nil,
			idToTID:      make(map[string]string),
			finalBaseUrl: "http://somethingelse:9090/__special-reports-transformer/transformers/special-reports/",
		},
		{
			clusterUrl:   "http://localhost:8080",
			baseUrl:      "/__special-reports-transformer/transformers/topics/",
			conceptType:  "topics",
			ids:          []string{},
			throttle:     1,
			createErr:    nil,
			idToTID:      make(map[string]string),
			finalBaseUrl: "http://localhost:8080/__special-reports-transformer/transformers/topics/",
		},
		{
			clusterUrl:  "http://ip-172-24-158-162.eu-west-1.compute.internal:8080",
			baseUrl:     "/__topics-transformer/transformers/topics/",
			conceptType: "topics",
			ids:         []string{"1", "2"},
			throttle:    1,
			createErr:   nil,
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
		var mockQueueSer queue = allOkQueue{}
		var mockHttpSer caller = nilHttpService{}
		pubService := newPublishService(clusterUrl, &mockQueueSer, &mockHttpSer)
		testBaseUrl, err := url.Parse(test.baseUrl)
		if err != nil {
			t.Error(err)
		}
		finalBaseUrl, err := url.Parse(test.finalBaseUrl)
		if err != nil {
			t.Error(err)
		}

		actualJob, err := pubService.createJob(test.ids, *testBaseUrl, test.throttle)

		if err != nil {
			if test.createErr != nil {
				if !strings.HasPrefix(err.Error(), test.createErr.Error()) {
					t.Fatalf("unexpected error. diff got vs want:\n%v\n%v", err, test.createErr)
				}
				continue
			}
			t.Errorf("unexpected error: %v", err)
			return
		}
		expectedJob := job{
			JobID:       actualJob.JobID,
			ConceptType: test.conceptType,
			IDToTID:     test.idToTID,
			URL:         *finalBaseUrl,
			Throttle:    test.throttle,
			Progress:    0,
			Status:      defined,
			FailedIDs:   []string{},
		}
		if !reflect.DeepEqual(*actualJob, expectedJob) {
			t.Errorf("wrong job. diff got vs want:\n%v\n%v", *actualJob, expectedJob)
		}
	}
}

func TestDeleteJob(t *testing.T) {
	tests := []struct {
		jobIDToDelete string
		jobs          map[string]*job
		nJobsAfter    int
		deleteErr     error
	}{
		{
			jobIDToDelete: "job_1",
			jobs: map[string]*job{
				"job_1": &job{
					JobID:       "job_1",
					ConceptType: "special-reports",
					IDToTID:     make(map[string]string),
					Throttle:    1,
					Status:      completed,
					FailedIDs:   []string{},
				},
			},
			nJobsAfter: 0,
			deleteErr:  nil,
		},
		{
			jobIDToDelete: "job_1",
			jobs: map[string]*job{
				"job_1": &job{
					JobID:       "job_1",
					ConceptType: "special-reports",
					IDToTID:     make(map[string]string),
					Throttle:    1,
					Status:      inProgress,
					FailedIDs:   []string{},
				},
			},
			nJobsAfter: 0,
			deleteErr:  errors.New(`message="Job is in progress, locked."`),
		},
		{
			jobIDToDelete: "job_99",
			jobs:          map[string]*job{},
			nJobsAfter:    0,
			deleteErr:     errors.New(`message="Job not found"`),
		},
	}
	for _, test := range tests {
		clusterUrl, err := url.Parse("http://localhost:8080/")
		if err != nil {
			t.Fatal(err)
		}
		var mockQueueSer queue = allOkQueue{}
		var mockHttpSer caller = nilHttpService{}
		pubService := publishService{
			clusterRouterAddress: clusterUrl,
			queueServiceI:        &mockQueueSer,
			jobs:                 test.jobs,
			httpService:          &mockHttpSer,
		}

		err = pubService.deleteJob(test.jobIDToDelete)

		if err != nil {
			if test.deleteErr != nil {
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
		name                    string
		baseURL                 string
		throttle                int
		definedIdsToResolvedIds map[string]string
		reloadErr               error
		idsFailure              *failure
		staticIds               string
		countFailure            *failure
		queueSer                queue
		idToTID                 map[string]string
		publishedIds            []string
		failedIds               []string
		status                  string
	}{
		{
			name:    "one",
			baseURL: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics",
			definedIdsToResolvedIds: map[string]string{
				"1": "1",
				"2": "2",
			},
			queueSer:     allOkQueue{},
			idToTID:      map[string]string{},
			publishedIds: []string{"1", "2"},
			failedIds:    []string{},
			status:       completed,
		},
		{
			name:    "two",
			baseURL: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics",
			definedIdsToResolvedIds: map[string]string{
				"1": "1",
				"2": "2",
			},
			reloadErr:    errors.New("Can't reload"),
			queueSer:     allOkQueue{},
			idToTID:      map[string]string{},
			publishedIds: []string{"1", "2"},
			failedIds:    []string{},
			status:       completed,
		},
		{
			name:    "three",
			baseURL: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics",
			definedIdsToResolvedIds: map[string]string{
				"1": "X1",
				"2": "X2",
			},
			queueSer:     allOkQueue{},
			idToTID:      map[string]string{},
			publishedIds: []string{"X1", "X2"},
			failedIds:    []string{},
			status:       completed,
		},
		{
			name:    "four",
			baseURL: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics",
			definedIdsToResolvedIds: map[string]string{
				"1": "1",
				"2": "2",
			},
			idsFailure: &failure{
				conceptID: "",
				error:     errors.New("Some error in ids."),
			},
			queueSer:     allOkQueue{},
			idToTID:      map[string]string{},
			publishedIds: []string{},
			failedIds:    []string{"", ""},
			status:       completed,
		},
		{
			name:    "five",
			baseURL: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics",
			definedIdsToResolvedIds: map[string]string{
				"1": "1",
				"2": "\"2",
			},
			queueSer:     allOkQueue{},
			idToTID:      map[string]string{},
			publishedIds: []string{"1"},
			failedIds:    []string{"2"},
			status:       completed,
		},
		{
			name:    "six",
			baseURL: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics",
			definedIdsToResolvedIds: map[string]string{
				"1": "1",
				"3": "3",
			},
			queueSer: allOkQueue{},
			idToTID: map[string]string{
				"1": "",
				"2": "",
				"3": "",
			},
			publishedIds: []string{"1", "3"},
			failedIds:    []string{"2"},
			status:       completed,
		},
		{
			name:    "seven",
			baseURL: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics",
			definedIdsToResolvedIds: map[string]string{
				"1": "1",
				"2": "2",
			},
			queueSer:     errorQueue{},
			idToTID:      map[string]string{},
			publishedIds: []string{},
			failedIds:    []string{"1", "2"},
			status:       completed,
		},
		{
			name:    "eight",
			baseURL: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics",
			definedIdsToResolvedIds: map[string]string{
				"1": "1",
				"2": "2",
			},
			countFailure: &failure{
				conceptID: "",
				error:     errors.New("Some error in count."),
			},
			queueSer:     allOkQueue{},
			idToTID:      map[string]string{},
			publishedIds: []string{},
			failedIds:    []string{},
			status:       failed,
		},
		{
			name:    "nine",
			baseURL: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics",
			definedIdsToResolvedIds: map[string]string{
				"1": "1",
				"2": "2",
			},
			staticIds: `{"id":"1"}
			//
			{"xx":"2"}`,
			queueSer:     allOkQueue{},
			idToTID:      map[string]string{},
			publishedIds: []string{"1"},
			failedIds:    []string{""},
			status:       completed,
		},
		{
			name:    "throttle",
			baseURL: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics",
			throttle: 10,
			definedIdsToResolvedIds: map[string]string{
				"1": "1",
				"2": "2",
			},
			queueSer:     allOkQueue{},
			idToTID:      map[string]string{},
			publishedIds: []string{"1", "2"},
			failedIds:    []string{},
			status:       completed,
		},
	}
	for _, test := range tests {
		//t.Run(fmt.Sprintf("Running: %s", test.name), func(t *testing.T) {
		clusterUrl, err := url.Parse("http://ip-172-24-158-162.eu-west-1.compute.internal:8080")
		if err != nil {
			t.Fatal(err)
		}
		var mockQueueSer queue = test.queueSer
		var mockHttpSer caller = definedIdsHttpService{
			definedToResolvedIs: test.definedIdsToResolvedIds,
			reloadF: func(string, string) error {
				return test.reloadErr
			},
			idsFailure:   test.idsFailure,
			countFailure: test.countFailure,
			staticIds:    test.staticIds,
		}
		if err != nil {
			t.Fatalf("unexpected error. %v", err)
		}
		testBaseUrl, err := url.Parse(test.baseURL)
		if err != nil {
			t.Fatalf("unexpected error. %v", err)
		}
		oneJob := &job{
			JobID:       "job_1",
			URL:         *testBaseUrl,
			ConceptType: "topics",
			IDToTID:     test.idToTID,
			Throttle:    test.throttle,
			Status:      defined,
			FailedIDs:   []string{},
		}

		pubService := publishService{
			clusterRouterAddress: clusterUrl,
			queueServiceI:        &mockQueueSer,
			jobs: map[string]*job{
				"job_1": oneJob,
			},
			httpService: &mockHttpSer,
		}
		pubService.runJob(oneJob, "Basic 1234")
		if test.throttle > 0 {
			time.Sleep(time.Millisecond * (500 + time.Duration(int(1000 * float64(len(test.publishedIds) + len(test.failedIds)) / float64(test.throttle)))))
		}
		var found bool
		for _, failedId := range test.failedIds {
			found = false
			for _, actualFailedId := range oneJob.FailedIDs {
				if reflect.DeepEqual(failedId, actualFailedId) {
					found = true
					break
				}

			}
			if !found {
				t.Errorf("Expected failed id %v couldn't be found in actual failures:", failedId)
			}
		}
		for _, expectedPublishedId := range test.publishedIds {
			_, ok := oneJob.IDToTID[expectedPublishedId]
			if !ok {
				t.Errorf("id %s didn't publish", expectedPublishedId)
			}
		}
		if oneJob.Status != test.status {
			t.Errorf("bad status. got %s, want %s", oneJob.Status, test.status)
		}
		//})
	}
}

type allOkQueue struct{}

func (q allOkQueue) sendMessage(id string, conceptType string, tid string, payload []byte) error {
	return nil
}

type errorQueue struct{}

func (q errorQueue) sendMessage(id string, conceptType string, tid string, payload []byte) error {
	return errors.New("Couldn't send because test.")
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

type nilHttpService struct{}

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

type definedIdsHttpService struct {
	definedToResolvedIs map[string]string
	reloadF             func(string, string) error
	idsFailure          *failure
	staticIds           string
	countFailure        *failure
}

func (h definedIdsHttpService) reload(url string, authorization string) error {
	return h.reloadF(url, authorization)
}

func (h definedIdsHttpService) getCount(url string, authorization string) (int, error) {
	if h.countFailure != nil {
		return -1, h.countFailure
	}
	return len(h.definedToResolvedIs), nil
}

func (h definedIdsHttpService) getIds(url string, authorization string) ([]byte, *failure) {
	if h.idsFailure != nil {
		return nil, h.idsFailure
	}
	if h.staticIds != "" {
		return []byte(h.staticIds), nil
	}
	allIdsResp := ""
	for definedId := range h.definedToResolvedIs {
		allIdsResp += fmt.Sprintf("{\"id\":\"%s\"}\n", definedId)
	}
	return []byte(allIdsResp), nil
}

func (h definedIdsHttpService) fetchConcept(id string, url string, auth string) ([]byte, *failure) {
	for definedId := range h.definedToResolvedIs {
		if reflect.DeepEqual(id, definedId) {
			return []byte(fmt.Sprintf("{\"uuid\": \"%s\"}", h.definedToResolvedIs[definedId])), nil
		}

	}
	return []byte{}, &failure{
		conceptID: id,
		error:     fmt.Errorf("Requested something that shouldn't have: %s", id),
	}
}
