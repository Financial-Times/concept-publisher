package main

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

type httpClient interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

type httpQueue struct {
	httpClient httpClient
	endpoint   string
}

func newHttpQueueService(httpClient httpClient, endpoint string) queue {
	return &httpQueue{
		httpClient: httpClient,
		endpoint:   endpoint,
	}
}

func (q *httpQueue) sendMessage(id string, conceptType string, tid string, payload []byte) error {

	url, err := url.Parse(strings.TrimRight(q.endpoint, "/") + "/" + id)
	if err != nil {
		log.Errorf("Error generating URL: %s", err)
		return err
	}

	req := &http.Request{
		Method: "PUT",
		URL:    url,
		Header: http.Header{
			"Message-Type":      {conceptType},
			"Content-Type":      {"application/json"},
			"X-Request-Id":      {tid},
			"Origin-System-Id":  {"http://cmdb.ft.com/systems/upp"},
			"Message-Timestamp": {time.Now().Format(messageTimestampDateFormat)},
		},
		Body:          ioutil.NopCloser(bytes.NewBuffer(payload)),
		ContentLength: int64(len(payload)),
	}

	log.Infof("Sending concept=[%s] uuid=[%s] tid=[%v] to %s", conceptType, id, tid, url)
	resp, err := q.httpClient.Do(req)
	if err != nil {
		log.Errorf("Error in making request [%d]: %s", resp.StatusCode, err)
		return err
	}
	defer resp.Body.Close()

	return nil
}
