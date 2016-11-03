package main

import (
	_ "net/http/pprof"
	"os"
	"net/http"
	"net/url"
	"github.com/Financial-Times/message-queue-go-producer/producer"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"net"
	"time"
)

func main() {
	app := cli.App("concept-publisher", "Retrieves concepts and puts them on a queue")
	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
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
		Value:  "NativeCmsMetadataPublicationEvents",
		Desc:   "The topic to write the V1 metadata to",
		EnvVar: "TOPIC",
	})
	clusterAddress := app.String(cli.StringOpt{
		Name:   "cluster-address",
		Value:  "http://ip-172-24-90-237.eu-west-1.compute.internal:8080",
		Desc:   "The hostname of one machine in the cluster, so that we can access any of the transformers by going to vulcan.",
		EnvVar: "CLUSTER_ADDRESS",
	})
	app.Action = func() {
		messageProducer := producer.NewMessageProducer(producer.MessageProducerConfig{Addr: *proxyAddress, Topic: *topic})
		clusterAddress, err := url.Parse(*clusterAddress)
		if err != nil {
			log.Fatalf("Invalid transformer URL: %v (%v)", *clusterAddress, err)
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
		publishService := &newPublishService(clusterAddress, messageProducer, httpClient)
		healthcheckHandler := &newHealthcheck(topic, proxyAddress, httpClient)
		assignHandlers(*port, publishService, healthcheckHandler)
	}
	app.Run(os.Args)
}

func assignHandlers(port string, publisherHandler *publisher, healthcheckHandler *healthcheck) {
	m := mux.NewRouter()
	http.Handle("/", handlers.CombinedLoggingHandler(os.Stdout, m))
	m.HandleFunc("/jobs", publisherHandler.createJob).Methods("POST")
	m.HandleFunc("/jobs", publisherHandler.listJobs).Methods("GET")
	m.HandleFunc("/jobs/{id}", publisherHandler.status).Methods("GET")
	m.HandleFunc("/__health", healthcheckHandler.health)
	m.HandleFunc("/__gtg", healthcheckHandler.gtg)
	log.Infof("Listening on [%v].\n", port)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Printf("Web server failed: [%v].\n", err)
	}
}
