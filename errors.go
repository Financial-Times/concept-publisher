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

type conflictError struct {
	msg string
}

func newConflictError(jobID string) *conflictError {
	return &conflictError{msg: fmt.Sprintf("message=\"Job is in progress, locked.\" jobId=%s", jobID)}
}

func (e conflictError) Error() string {
	return e.msg
}
