package main

import "fmt"

type notFoundError struct {
	msg string
}

func newNotFoundError(jobID string) *notFoundError {
	return &notFoundError{msg: fmt.Sprintf("message=\"Job not found\" jobId=%s", jobID)}
}

func (e notFoundError) Error() string {
	return e.msg
}

type failure struct {
	conceptID string
	error     error
}

func newFailure(conceptID string, err error) *failure {
	return &failure{conceptID: conceptID, error: err}
}

func (f *failure) Error() string {
	return f.error.Error()
}
