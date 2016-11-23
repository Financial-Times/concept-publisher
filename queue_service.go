package main

import (
	"github.com/Financial-Times/message-queue-go-producer/producer"
	"time"
)

type queueService struct {
	producer *producer.MessageProducer
}

func newQueueService(producer *producer.MessageProducer) *queueService {
	return &queueService{producer: producer}
}

type queueServiceI interface {
	sendMessage(id string, conceptType string, tid string, payload []byte)
}

func (q *queueService) sendMessage(id string, conceptType string, tid string, payload []byte) error {
	message := producer.Message{
		Headers: buildHeader(id, conceptType, tid),
		Body:    string(payload),
	}
	return (*q.producer).SendMessage(id, message)
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