package main

import (
	"github.com/Financial-Times/message-queue-go-producer/producer"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"strconv"
	"time"
)

func main() {
	app := cli.App("concept-publisher", "Retrieves concepts and puts them on a queue")
	port := app.Int(cli.IntOpt{
		Name:   "port",
		Value:  8080,
		Desc:   "Port to listen on",
		EnvVar: "PORT",
	})
	proxyAddress := app.String(cli.StringOpt{
		Name:   "proxy-address",
		Value:  "http://localhost:8080",
		Desc:   "Address used by the producer to connect to the queue",
		EnvVar: "PROXY_ADDRESS",
	})
	topic := app.String(cli.StringOpt{
		Name:   "destination-topic",
		Value:  "Concepts",
		Desc:   "The topic to write the V1 metadata to. (e.g. Concepts)",
		EnvVar: "TOPIC",
	})
	clusterRouterAddress := app.String(cli.StringOpt{
		Name:   "cluster-router-address",
		Value:  "http://ip-172-24-90-237.eu-west-1.compute.internal:8080",
		Desc:   "The hostname and port to the router of the cluster, so that we can access any of the transformers by going to vulcan. (e.g. http://ip-172-24-90-237.eu-west-1.compute.internal:8080)",
		EnvVar: "CLUSTER_ROUTER_ADDRESS",
	})
	app.Action = func() {
		messageProducer := producer.NewMessageProducer(producer.MessageProducerConfig{Addr: *proxyAddress, Topic: *topic})
		clusterRouterAddress, err := url.Parse(*clusterRouterAddress)
		if err != nil {
			log.Fatalf("Invalid clusterRouterAddress=%v %v", *clusterRouterAddress, err)
		}
		httpClient := &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 128,
				Dial: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).Dial,
			},
		}
		var queueSerI queue = newQueueService(&messageProducer)
		var httpSerI caller = newHttpCaller(httpClient)
		var pubSerI publisher = newPublishService(clusterRouterAddress, &queueSerI, &httpSerI)
		healthHandler := newHealthcheckHandler(*topic, *proxyAddress, httpClient)
		pubHandler := newPublishHandler(&pubSerI)
		assignHandlers(*port, &pubHandler, &healthHandler)
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalf("Couldn't start up application: %v", err)
	}
}

func assignHandlers(port int, publisherHandler *publishHandler, healthcheckHandler *healthcheckHandler) {
	m := mux.NewRouter()
	http.Handle("/", handlers.CombinedLoggingHandler(os.Stdout, m))
	m.HandleFunc("/jobs", publisherHandler.createJob).Methods("POST")
	m.HandleFunc("/jobs", publisherHandler.listJobs).Methods("GET")
	m.HandleFunc("/jobs/{id}", publisherHandler.status).Methods("GET")
	m.HandleFunc("/jobs/{id}", publisherHandler.deleteJob).Methods("DELETE")
	m.HandleFunc("/__health", healthcheckHandler.health())
	m.HandleFunc("/__gtg", healthcheckHandler.gtg)
	log.Infof("Listening on [%v].\n", port)
	err := http.ListenAndServe(":"+strconv.Itoa(port), nil)
	if err != nil {
		log.Printf("Web server failed: [%v].\n", err)
	}
}
