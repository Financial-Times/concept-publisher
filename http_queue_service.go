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

type httpQueue struct {
	httpClient *http.Client
	endpoint   string
}

func newHttpQueueService(httpClient *http.Client, endpoint string) httpQueue {

	return httpQueue{
		httpClient: httpClient,
		endpoint:   endpoint,
	}
}

func (q httpQueue) sendMessage(id string, conceptType string, tid string, payload []byte) error {

	url, err := url.Parse(strings.TrimRight(q.endpoint, "/") + "/" + id)
	if err != nil {
		log.Errorf("Error generating URL: %s", err)
		return err
	}
	log.Debug(url)
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

	resp, err := q.httpClient.Do(req)
	if err != nil {
		log.Errorf("Error in making request [%d]: %s", resp.StatusCode, err)
		return err
	}

	return nil
}
