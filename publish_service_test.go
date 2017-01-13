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
	pubService := newPublishService(url, &mockQueueSer, &mockHttpSer, 1)
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
		name         string
		clusterUrl   string
		baseUrl      string
		gtgUrl       string
		conceptType  string
		ids          []string
		throttle     int
		createErr    error
		definedIDs   []string
		finalBaseUrl string
		finalGtgUrl string
	}{
		{
			name:         "one",
			clusterUrl:   "http://localhost:8080",
			baseUrl:      "http://localhost:8080/__special-reports-transformer/transformers/special-reports/",
			gtgUrl:       "http://localhost:8080/__special-reports-transformer/__gtg",
			conceptType:  "special-reports",
			ids:          []string{},
			throttle:     1,
			createErr:    nil,
			definedIDs:   []string{},
			finalBaseUrl: "http://localhost:8080/__special-reports-transformer/transformers/special-reports/",
			finalGtgUrl:  "http://localhost:8080/__special-reports-transformer/__gtg",
		},
		{
			name:         "two",
			clusterUrl:   "http://ip-172-24-158-162.eu-west-1.compute.internal:8080",
			baseUrl:      "/__special-reports-transformer/transformers/special-reports",
			gtgUrl:       "/__special-reports-transformer/__gtg",
			conceptType:  "special-reports-otherwise",
			ids:          []string{},
			throttle:     1,
			createErr:    nil,
			definedIDs:   []string{},
			finalBaseUrl: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__special-reports-transformer/transformers/special-reports",
			finalGtgUrl:  "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__special-reports-transformer/__gtg",
		},
		{
			name:         "three",
			clusterUrl:   "http://ip-172-24-158-162.eu-west-1.compute.internal:8080",
			baseUrl:      "http://somethingelse:9090/__special-reports-transformer/transformers/special-reports/",
			gtgUrl:       "http://somethingelse:9090/__special-reports-transformer/__gtg",
			conceptType:  "special-reports",
			ids:          []string{},
			throttle:     1,
			createErr:    nil,
			definedIDs:   []string{},
			finalBaseUrl: "http://somethingelse:9090/__special-reports-transformer/transformers/special-reports/",
			finalGtgUrl:  "http://somethingelse:9090/__special-reports-transformer/__gtg",
		},
		{
			name:         "four",
			clusterUrl:   "http://localhost:8080",
			baseUrl:      "/__special-reports-transformer/transformers/topics/",
			gtgUrl:       "/__special-reports-transformer/__gtg",
			conceptType:  "topics",
			ids:          []string{},
			throttle:     1,
			createErr:    nil,
			definedIDs:   []string{},
			finalBaseUrl: "http://localhost:8080/__special-reports-transformer/transformers/topics/",
			finalGtgUrl:     "http://localhost:8080/__special-reports-transformer/__gtg",
		},
		{
			name:         "five",
			clusterUrl:  "http://ip-172-24-158-162.eu-west-1.compute.internal:8080",
			baseUrl:     "/__topics-transformer/transformers/topics/",
			gtgUrl:      "/__topics-transformer/__gtg",
			conceptType: "topics",
			ids:         []string{"1", "2"},
			throttle:    1,
			createErr:   nil,
			definedIDs:  []string{"1", "2"},
			finalBaseUrl: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics/",
			finalGtgUrl:  "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/__gtg",
		},
	}
	for _, test := range tests {
		clusterUrl, err := url.Parse(test.clusterUrl)
		if err != nil {
			t.Error(err)
		}
		var mockQueueSer queue = allOkQueue{}
		var mockHttpSer caller = nilHttpService{}
		pubService := newPublishService(clusterUrl, &mockQueueSer, &mockHttpSer, 1)
		actualJob, err := pubService.createJob(test.conceptType, test.ids, test.baseUrl, test.gtgUrl, test.throttle)
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
			IDs:         test.definedIDs,
			URL:         test.finalBaseUrl,
			GtgURL:      test.finalGtgUrl,
			Throttle:    test.throttle,
			Progress:    0,
			Status:      defined,
			FailedIDs:   []string{},
		}
		if !reflect.DeepEqual(*actualJob, expectedJob) {
			t.Errorf("test %v - wrong job. diff got vs want:\n%v\n%v", test.name, *actualJob, expectedJob)
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
					IDs:         []string{},
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
					IDs:         []string{},
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
			queueService:        &mockQueueSer,
			jobs:                 test.jobs,
			httpService:          &mockHttpSer,
			gtgRetries:           1,
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
		gtgErr                  error
		idsFailure              *failure
		staticIds               string
		countFailure            *failure
		queueSer                queue
		definedIDs              []string
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
			definedIDs:   []string{},
			publishedIds: []string{"1", "2"},
			failedIds:    []string{},
			status:       completed,
		},
		{
			name:    "reload error to ignore",
			baseURL: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics",
			definedIdsToResolvedIds: map[string]string{
				"1": "1",
				"2": "2",
			},
			reloadErr:    errors.New("Can't reload"),
			queueSer:     allOkQueue{},
			definedIDs:   []string{},
			publishedIds: []string{"1", "2"},
			failedIds:    []string{},
			status:       completed,
		},
		{
			name:    "reload ok but gtg times out",
			baseURL: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics",
			definedIdsToResolvedIds: map[string]string{
				"1": "1",
				"2": "2",
			},
			gtgErr:       errors.New("Timed out"),
			queueSer:     allOkQueue{},
			definedIDs:   []string{},
			publishedIds: []string{},
			failedIds:    []string{},
			status:       failed,
		},
		{
			name:    "three",
			baseURL: "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics",
			definedIdsToResolvedIds: map[string]string{
				"1": "X1",
				"2": "X2",
			},
			queueSer:     allOkQueue{},
			definedIDs:   []string{},
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
			definedIDs:   []string{},
			publishedIds: []string{},
			failedIds:    []string{},
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
			definedIDs:   []string{},
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
			definedIDs: []string{"1","2","3"},
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
			definedIDs:   []string{},
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
			definedIDs:   []string{},
			publishedIds: []string{"1", "2"},
			failedIds:    []string{},
			status:       completed,
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
			definedIDs:   []string{},
			publishedIds: []string{"1"},
			failedIds:    []string{""},
			status:       completed,
		},
		{
			name:     "throttle",
			baseURL:  "http://ip-172-24-158-162.eu-west-1.compute.internal:8080/__topics-transformer/transformers/topics",
			throttle: 10,
			definedIdsToResolvedIds: map[string]string{
				"1": "1",
				"2": "2",
			},
			queueSer:     allOkQueue{},
			definedIDs:   []string{},
			publishedIds: []string{"1", "2"},
			failedIds:    []string{},
			status:       completed,
		},
	}
	for _, test := range tests {
		clusterUrl, err := url.Parse("http://ip-172-24-158-162.eu-west-1.compute.internal:8080")
		if err != nil {
			t.Fatal(err)
		}
		var mockQueueSer queue = test.queueSer
		var mockHttpSer caller = definedIdsHttpService{
			definedToResolvedIs: test.definedIdsToResolvedIds,
			reloadF:             func(string, string) error {
				return test.reloadErr
			},
			gtgErr:              test.gtgErr,
			idsFailure:          test.idsFailure,
			countFailure:        test.countFailure,
			staticIds:           test.staticIds,
		}
		oneJob := &job{
			JobID:       "job_1",
			URL:         test.baseURL,
			ConceptType: "topics",
			IDs:         test.definedIDs,
			Throttle:    test.throttle,
			Status:      defined,
			FailedIDs:   []string{},
		}

		pubService := publishService{
			clusterRouterAddress: clusterUrl,
			queueService:        &mockQueueSer,
			jobs: map[string]*job{
				"job_1": oneJob,
			},
			httpService: &mockHttpSer,
			gtgRetries:  1,
		}
		pubService.runJob(oneJob, "Basic 1234")
		if test.throttle > 0 {
			time.Sleep(time.Millisecond * (500 + time.Duration(int(1000*float64(len(test.publishedIds)+len(test.failedIds))/float64(test.throttle)))))
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
				t.Errorf("%v - Expected failed id %v couldn't be found in actual failures:", test.name, failedId)
			}
		}
		if oneJob.Status != test.status {
			t.Errorf("%v - bad status. got %s, want %s", test.name, oneJob.Status, test.status)
		}
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

func (h nilHttpService) checkGtg(url string) error {
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
	gtgErr              error
	idsFailure          *failure
	staticIds           string
	countFailure        *failure
}

func (h definedIdsHttpService) reload(url string, authorization string) error {
	return h.reloadF(url, authorization)
}

func (h definedIdsHttpService) checkGtg(url string) error {
	return h.gtgErr
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
