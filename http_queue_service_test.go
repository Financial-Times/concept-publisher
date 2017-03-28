package main

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"

	"errors"

	"github.com/stretchr/testify/assert"
)

type mockHttpClient struct {
	resp       string
	statusCode int
	err        error
}

func (c *mockHttpClient) Do(req *http.Request) (resp *http.Response, err error) {
	cb := ioutil.NopCloser(bytes.NewReader([]byte(c.resp)))
	return &http.Response{Body: cb, StatusCode: c.statusCode}, c.err
}

func TestHttpQueueService(t *testing.T) {

	hq := httpQueue{&mockHttpClient{"Success", 200, nil}, "http://localhost/endpoint"}

	err := hq.sendMessage("1", "Brands", "tid_1", []byte{1, 2})

	assert.NoError(t, err)

}

func TestHttpQueueServiceBadURL(t *testing.T) {

	hq := httpQueue{&mockHttpClient{"Failure", 0, nil}, "https://192.16 8.0.2:8080/foo"}

	err := hq.sendMessage("1", "Brands", "tid_1", []byte{1, 2})

	assert.Error(t, err)

}

func TestHttpQueueServiceBadResponse(t *testing.T) {

	hq := httpQueue{&mockHttpClient{"Failure", 404, errors.New("No page found")}, "https://192.168.0.2:8080/foo"}

	err := hq.sendMessage("1", "Brands", "tid_1", []byte{1, 2})

	assert.Error(t, err)

}

func TestNewHttpQueueService(t *testing.T) {
	actualHQ := newHttpQueueService(&mockHttpClient{}, "endpoint")
	expectedHQ := httpQueue{&mockHttpClient{}, "endpoint"}

	assert.Equal(t, expectedHQ, actualHQ)
}
