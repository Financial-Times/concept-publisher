package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
)

type httpCaller struct {
	httpClient *http.Client
}

func newHttpCaller(httpClient *http.Client) httpCaller {
	return httpCaller{httpClient: httpClient}
}

type caller interface {
	reload(url string, authorization string) error
	checkGtg(url string) error
	getIds(url string, authorization string) ([]byte, *failure)
	getCount(url string, authorization string) (int, error)
	fetchConcept(conceptID string, url string, authorization string) ([]byte, *failure)
}

func (h httpCaller) reload(url string, authorization string) error {
	req, _ := http.NewRequest("POST", url, nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("message=\"Could not connect to reload concepts\" url=\"%v\" err=\"%s\"", url, err)
	}
	defer closeNice(resp)
	if resp.StatusCode / 100 != 2 {
		return fmt.Errorf("message=\"Incorrect status when reloading concepts\" status=%d url=\"%s\"", resp.StatusCode, url)
	}
	return nil
}

func (h httpCaller) checkGtg(url string) error {
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Could not connect to good-to-go endpoint url=%v err=%s", url, err)
	}
	defer closeNice(resp)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Incorrect status on good-to-go status=%d url=%s", resp.StatusCode, url)
	}
	return nil
}

func (h httpCaller) getIds(url string, authorization string) ([]byte, *failure) {
	req, _ := http.NewRequest("GET", url, nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, newFailure("", fmt.Errorf("Could not get /__ids from: %v (%v)", url, err))
	}
	defer closeNice(resp)
	if resp.StatusCode != http.StatusOK {
		return nil, newFailure("", fmt.Errorf("Could not get /__ids from %v. Returned %v", url, resp.StatusCode))
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, newFailure("", fmt.Errorf("message=\"Could not read /__ids response\" %v", err))
	}
	return body, nil
}

func (h httpCaller) getCount(url string, authorization string) (int, error) {
	req, _ := http.NewRequest("GET", url, nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return -1, fmt.Errorf("Could not connect to %v. Error (%v)", url, err)
	}
	defer closeNice(resp)
	if resp.StatusCode != http.StatusOK {
		return -1, fmt.Errorf("Could not get count from %v. Returned %v", url, resp.StatusCode)
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return -1, fmt.Errorf("Could not read count from %v. Error (%v)", url, err)
	}
	count, err := strconv.Atoi(string(respBody))
	if err != nil {
		return -1, fmt.Errorf("Could not convert payload (%v) to int. Error (%v)", string(respBody), err)
	}
	return count, nil
}

func (h httpCaller) fetchConcept(conceptID string, url string, authorization string) ([]byte, *failure) {
	req, _ := http.NewRequest("GET", url, nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, newFailure(conceptID, fmt.Errorf("message=\"Could not make HTTP request to fetch a concept\" conceptId=%v %v", conceptID, err))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, newFailure(conceptID, fmt.Errorf("Fetching a concept resulted in not ok response conceptId=%v url=%v status=%v", conceptID, url, resp.StatusCode))
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, newFailure(conceptID, fmt.Errorf("message=\"Could not read concept from response while fetching\" uuid=%v %v", conceptID, err))
	}
	err = resp.Body.Close()
	if err != nil {
		return nil, newFailure(conceptID, fmt.Errorf("message=\"Could not close response while fetching\" uuid=%v %v", conceptID, err))
	}
	return data, nil
}
