package main

import (
	"github.com/Financial-Times/message-queue-go-producer/producer"
	log "github.com/Sirupsen/logrus"
	"time"
)

type kafkaQueue struct {
	producer *producer.MessageProducer
}

func newQueueService(producer *producer.MessageProducer) kafkaQueue {
	return kafkaQueue{producer: producer}
}

type queue interface {
	sendMessage(id string, conceptType string, tid string, payload []byte) error
}

func (q kafkaQueue) sendMessage(uuid string, conceptType string, tid string, payload []byte) error {
	log.Infof("Sending concept=[%s] uuid=[%s] tid=[%v]", uuid, conceptType, tid)
	message := producer.Message{
		Headers: buildHeader(uuid, conceptType, tid),
		Body:    string(payload),
	}
	return (*q.producer).SendMessage(uuid, message)
}

func buildHeader(uuid string, conceptType string, tid string) map[string]string {
	return map[string]string{
		"Message-Id":        uuid,
		"Message-Type":      conceptType,
		"Content-Type":      "application/json",
		"X-Request-Id":      tid,
		"Origin-System-Id":  "http://cmdb.ft.com/systems/upp",
		"Message-Timestamp": time.Now().Format(messageTimestampDateFormat),
	}
}
