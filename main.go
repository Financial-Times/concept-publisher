package main

import (
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"time"

	"net/url"

	"github.com/Financial-Times/message-queue-go-producer/producer"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
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
		Desc:   "Address used by the producer to connect to the queue",
		EnvVar: "PROXY_ADDRESS",
	})
	topic := app.String(cli.StringOpt{
		Name:   "destination-topic",
		Value:  "Concepts",
		Desc:   "The topic to write the V1 metadata to. (e.g. Concepts)",
		EnvVar: "TOPIC",
	})
	gtgRetries := app.Int(cli.IntOpt{
		Name:   "transformer-gtg-retries",
		Value:  60,
		Desc:   "The number of times concept-publisher should try to poll a transformer's good-to-go endpoint if that responds to __reload with 2xx status. It's doing the reload concurrently, that's why we're waiting, the question is how much. One period is 5 seconds.",
		EnvVar: "TRANSFORMER_GTG_RETRIES",
	})
	clusterRouterAddress := app.String(cli.StringOpt{
		Name:   "cluster-router-address",
		Desc:   "The hostname and port to the router of the cluster, so that we can access any of the transformers by going to vulcan. (e.g. http://ip-172-24-90-237.eu-west-1.compute.internal:8080)",
		EnvVar: "CLUSTER_ROUTER_ADDRESS",
	})
	s3RwAddress := app.String(cli.StringOpt{
		Name:   "s3-rw-address",
		Desc:   "Address used to connect to the S3 RW app",
		EnvVar: "S3_RW_ADDRESS",
	})
	app.Action = func() {
		var parsedClusterRouterAddress *url.URL
		if *clusterRouterAddress == "" {
			log.Infof("No clusterRouterAddress provided, accessing transformers through provided absolute URLs.")
		} else {
			var err error
			parsedClusterRouterAddress, err = url.Parse(*clusterRouterAddress)
			if err != nil {
				log.Fatalf("Invalid clusterRouterAddress=%v %v", *clusterRouterAddress, err)
			}
			log.Info("Valid clusterRouterAddress provided, accessing transformers through vulcan.")
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
		var queueService queue
		var messageProducer producer.MessageProducer
		if *proxyAddress != "" {
			messageProducer = producer.NewMessageProducer(producer.MessageProducerConfig{Addr: *proxyAddress, Topic: *topic})
			queueService = newQueueService(&messageProducer)
		} else if *s3RwAddress != "" {
			queueService = newHttpQueueService(httpClient, *s3RwAddress)
		}

		var httpCall caller = newHttpCaller(httpClient)
		var publishService publisher = newPublishService(parsedClusterRouterAddress, &queueService, &httpCall, *gtgRetries)
		hc := NewHealthCheck(messageProducer, *s3RwAddress, httpClient)
		pubHandler := newPublishHandler(&publishService)
		assignHandlers(*port, &pubHandler, hc)
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalf("Couldn't start up application: %v", err)
	}
}

func assignHandlers(port int, publisherHandler *publishHandler, hc *HealthCheck) {
	m := mux.NewRouter()
	http.Handle("/", handlers.CombinedLoggingHandler(os.Stdout, m))
	m.HandleFunc("/jobs", publisherHandler.createJob).Methods("POST")
	m.HandleFunc("/jobs", publisherHandler.listJobs).Methods("GET")
	m.HandleFunc("/jobs/{id}", publisherHandler.status).Methods("GET")
	m.HandleFunc("/jobs/{id}", publisherHandler.deleteJob).Methods("DELETE")
	m.HandleFunc("/__health", hc.Health())
	m.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(hc.GTG))
	m.HandleFunc(status.PingPath, status.PingHandler)
	m.HandleFunc(status.PingPathDW, status.PingHandler)
	m.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)
	m.HandleFunc(status.BuildInfoPathDW, status.BuildInfoHandler)
	log.Infof("Listening on [%v].\n", port)
	err := http.ListenAndServe(":"+strconv.Itoa(port), nil)
	if err != nil {
		log.Printf("Web server failed: [%v].\n", err)
	}
}
