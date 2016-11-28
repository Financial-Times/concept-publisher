# Concept publisher (concept-publisher)
[![CircleCI](https://circleci.com/gh/Financial-Times/concept-publisher.svg?style=svg)](https://circleci.com/gh/Financial-Times/concept-publisher) [![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/concept-publisher)](https://goreportcard.com/report/github.com/Financial-Times/concept-publisher) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/concept-publisher/badge.svg?branch=tests)](https://coveralls.io/github/Financial-Times/concept-publisher?branch=tests)
[![Circle CI](https://circleci.com/gh/Financial-Times/concept-publisher/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/concept-publisher/tree/master)

__Fetches concepts from concept-transformers and adds them to kafka.__

## Installation

`go get github.com/Financial-Times/concept-publisher`

## Running locally

```
go build

# Open tunnel to publishing cluster:
ssh -L 8083:localhost:8080 core@xp-tunnel-up.ft.com

# Set up tunnel for cluster address:
export CLUSTER_ROUTER_ADDRESS="http://localhost:8083"

./concept-publisher
```

## Endpoints

### /jobs

Return all the jobs' ids:

### GET /jobs/{id}

Get detailed job status, should it be in progress, completed or failed.

### POST /jobs

concept: organisations, people, subjects, locations...

* url: url to use to get the transformed concept
  * can either be absolute of relative - for relative the base url is CLUSTER_ROUTER_ADDRESS
  * {url}/__ids that lists the identities of the resources in the form '{"id":"abc"}\n{"id":"123"}'
  * {url}/{uid} that returns the transformed concept in UPP json format
  * {url}/__count returns the number of concepts
* ids (optional): list if ids to publish - if the list is not empty ids will not be looked up via __ids endpoint on the transformer and only the uuids from the list will be published
* throttle: no of req/s when calling the transformers to get transformed content
* authorization: authorization credentials if necessary - optional

Examples:

`curl -X POST -H "Content-Type: application/json" localhost:8080/jobs --data '{"concept":"organisations","url": "/__composite-orgs-transformer/transformers/organisations/", "throttle": 1000, "authorization": "Basic base64user:pass"}'`
   
`curl -X POST -H "Content-Type: application/json" localhost:8080/jobs --data '{"concept":"organisations","ids":["uuid1","uuid2"],"url": "http://specific-ftp2-host.ft.com/organisations/", "throttle": 1000, "authorization": "Basic base64user:pass"}'`
