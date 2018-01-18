# Concept publisher (concept-publisher)
[![CircleCI](https://circleci.com/gh/Financial-Times/concept-publisher.svg?style=svg)](https://circleci.com/gh/Financial-Times/concept-publisher) [![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/concept-publisher)](https://goreportcard.com/report/github.com/Financial-Times/concept-publisher) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/concept-publisher/badge.svg)](https://coveralls.io/github/Financial-Times/concept-publisher)

__Fetches concepts from concept-transformers and adds them to kafka.__

## Installation

```
go get -u github.com/kardianos/govendor
go get -u github.com/Financial-Times/concept-publisher
cd $GOPATH/src/github.com/Financial-Times/concept-publisher
govendor sync
go build .
```

## Configuration

### Vulcan-based routing
For environments using vulcan-based routing, set the `CLUSTER_ROUTER_ADDRESS`.

### URL-based routing
For environments using regular URLs, don't set the `CLUSTER_ROUTER_ADDRESS`.

## NOTE

This concept publishing pipeline is nearing end of life. It can currently only used to publish all organisations, factset people and authors. Most TME concepts have been switched over to use the new concept publishing pipeline described in detail [here](https://sites.google.com/a/ft.com/universal-publishing/documentation/introduction-to-metadata) which are published via the [basic-tme-transformer](https://github.com/Financial-Times/basic-tme-transformer)

## Endpoints

### GET /jobs

Return all the jobs' ids.

### POST /jobs

* concept: the name of the concept type. It's important because there are unusual cases when this name differs from what is in the URL path.
* url: url to use to get the transformed concept
  * if using vulcan-based routing
    * it can be a relative URL, whose base is the `CLUSTER_ROUTER_ADDRESS`
    * it can be an absolute URL
  * if using normal routing, it must be an absolute URL
  * {url}/__count returns the number of concepts
  * {url}/__ids that lists the identities of the resources in the form '{"id":"abc"}\n{"id":"def"}'
  * {url}/{uid} that returns the transformed concept in UPP json format
* gtgUrl: url to check that the transformer has finished reloading after a __reload call.
Not all applications expose a good-to-go endpoint, if you still want a successful publish, make sure they don't expose __reload either (than it doesn't come to ask for __gtg) or put a dummy endpoint that works and gives a 200.
* ids (optional): list if ids to publish - if the list is not empty ids will not be looked up via the __ids endpoint on the transformer and only the uuids from the list will be published
* throttle: no of req/s when calling the transformers to get transformed content
* authorization (optional)

#### Examples for the K8S (regular routing) stack

```
curl -X POST -H "Content-Type: application/json" localhost:8080/jobs --data '
{
  "concept": "special-reports",
  "url": "http://special-reports-transformer:8080/transformers/special-reports/",
  "gtgUrl": "http://special-reports-transformer:8080/__gtg",
  "throttle": 1000,
}'

curl -X POST -H "Content-Type: application/json" localhost:8080/jobs --data '
{
  "concept": "brands",
  "ids": ["uuid1", "uuid2"],
  "url": "https://brands-transformer-up.ft.com/transformers/brands/",
  "gtgUrl": "https://brands-transformer-up.ft.com/build-info",
  "throttle": 1000
  "authorization": "Basic base64user:pass"
}'
```

#### Examples for the current (CoCo, vulcan-routing) stack

```
curl -X POST -H "Content-Type: application/json" localhost:8080/jobs --data '
{
  "concept": "organisations",
  "url": "/__composite-orgs-transformer/transformers/organisations/",
  "gtgUrl": "/__composite-orgs-transformer/__gtg",
  "throttle": 1000,
  "authorization": "Basic base64user:pass"
  "ids": ["50a4bb3c-fc46-11e7-9dea-0401beb96201", "68ceb815-b81b-42f1-8f52-7e54f362a5d9"]
}'

curl -X POST -H "Content-Type: application/json" localhost:8080/jobs --data '
{
  "concept":"people", 
  "url": "/__v1-authors-transformer/transformers/authors/", 
  "gtgUrl": "/__v1-authors-transformer/__gtg"
  "throttle": 1000,
  "authorization": "Basic base64user:pass"
}'
```


### GET /jobs/{id}

You can add the parameter _full_ to see the optionally given defined IDs and the failedIDs of the job.

e.g. `curl -H "Accept: application/json" localhost:8080/jobs/job_123456?full`

Get detailed job status, should it be in progress, completed or failed.

### DELETE /jobs/{id}

Deletes the job. Works only if the job is terminated.
