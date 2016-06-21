# Concept publisher (concept-publisher)
[![Circle CI](https://circleci.com/gh/Financial-Times/concept-publisher/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/concept-publisher/tree/master)

__Retrives transformed concepts and add them to kafka.__

## Installation

For the first time:

`go get github.com/Financial-Times/concept-publisher`

or update:

`go get -u github.com/Financial-Times/concept-publisher`

## Running

The values below also represent the default values: 

```
export|set PROXY_ADDR=localhost:8080
export|set TRANSFORMER_ADDR=localhost:8080
export|set TOPIC=Concept

$GOPATH/bin/concept-publisher
```

With Docker:

`docker build -t coco/concept-publisher .`
`docker run -ti coco/concept-publisher`

## Endpoints

### /jobs
#### POST
concept: organisations, people, subjects, locations...
url: url to use to get the transformed concept
* can either be absolute of relative - for relative the base url is TRANSFORMER_ADDR
* {url}/__ids that lists the identities of the resources in the form '{"id":"abc"}\n{"id":"123"}'
* {url}/{uid} that returns the transformed concept in UPP json format
* {url}/__count returns the number of concepts - optional

throttle: no of req/s when calling the transformers to get transformed content  
authorization: authorization credentials if necessary - optional


Example:
`curl -X PUT -H "Content-Type: application/json" localhost:8080/jobs --data '{"concept":"organisations","url": "http://localhost:8080/transformers/organisations/", "throttle": 100, "authorization": "Basic base64user:pass"}'`
`{"jobId":"job_sMxULvEpjw"}`

#### GET
Gets all the jobs:
`[{"jobId":"job_XVlBzgbaiC"},{"jobId":"job_sMxULvEpjw"},{"jobId":"job_FKBAuIiPSO"},{"jobId":"job_ViPAxUKQsR"}]`


### /jobs/{id}
### GET
Get job status

`curl "localhost:9090/jobs/job_ViPAxUKQsR"`
`{"concept":"organisations","url":"http://localhost:8080/transformers/organisations/","throttle":100,"count":9859,"done":8865,"status":"In progress"}`