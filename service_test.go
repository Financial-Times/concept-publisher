package main

import (
	"fmt"
	"github.com/Financial-Times/message-queue-go-producer/producer"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

const (
	uuid1 = "01ac18d5-ea4b-3121-b03c-a652ea4ba335"
	uuid2 = "a12bd195-78c7-395c-804e-b77877934f3c"
)

func TestPublishService(t *testing.T) {

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, uuid1) {
			fmt.Fprintf(w, "{\"uuid\":\"%s\",\"properName\":\"Org1\"}\n", uuid1)
			return
		}
		if strings.Contains(r.URL.Path, uuid2) {
			fmt.Fprintf(w, "{\"uuid\":\"%s\",\"properName\":\"Org2\"}\n", uuid2)
			return
		}
		if strings.Contains(r.URL.Path, "/__ids") {
			fmt.Fprintf(w, "{\"id\":\"%s\"}\n", uuid1)
			fmt.Fprintf(w, "{\"id\":\"%s\"}\n", uuid2)
			return
		}
		if strings.Contains(r.URL.Path, "/__count") {
			fmt.Fprintf(w, "%d", 2)
			return
		}

	}))
	defer func() {
		ts.Close()
	}()

	u, _ := url.Parse(ts.URL + "/")
	p := &dummyProducer{msgs: make(map[string]producer.Message)}
	s := newPublishService(u, p)
	jobID := s.newJob("organisations", u, "", 1)
	var job jobStatus
	for i := 0; ; i++ {
		if i > 10 {
			t.Error("Job did not complete")
			return
		}
		job, _ = s.jobStatus(jobID)
		if job.Status == "Completed" {
			break
		}
		time.Sleep(1 * time.Second)
	}

	assert.EqualValues(t, jobStatus{URL: ts.URL + "/", Concept: "organisations", Count: 2, Throttle: 1, Done: 2, Status: "Completed"}, job)
	assert.Equal(t, 2, len(p.msgs), "Should have produced 2 messages")
	assert.Equal(t, uuid1, p.msgs[uuid1].Headers["Message-Id"])
	assert.Equal(t, "organisations", p.msgs[uuid1].Headers["Message-Type"])
	assert.Equal(t, "http://cmdb.ft.com/systems/upp", p.msgs[uuid1].Headers["Origin-System-Id"])
	assert.Equal(t, fmt.Sprintf("{\"uuid\":\"%s\",\"properName\":\"Org1\"}\n", uuid1), p.msgs[uuid1].Body)
	assert.Equal(t, uuid2, p.msgs[uuid2].Headers["Message-Id"])
	assert.Equal(t, "organisations", p.msgs[uuid2].Headers["Message-Type"])
	assert.Equal(t, "http://cmdb.ft.com/systems/upp", p.msgs[uuid2].Headers["Origin-System-Id"])
	assert.Equal(t, fmt.Sprintf("{\"uuid\":\"%s\",\"properName\":\"Org2\"}\n", uuid2), p.msgs[uuid2].Body)
}

type dummyProducer struct {
	msgs map[string]producer.Message
}

func (p *dummyProducer) SendMessage(key string, msg producer.Message) error {
	p.msgs[key] = msg
	return nil
}
