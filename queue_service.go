package main

import (
	"github.com/Financial-Times/message-queue-go-producer/producer"
	log "github.com/Sirupsen/logrus"
	"github.com/satori/go.uuid"
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

func (q kafkaQueue) sendMessage(id string, conceptType string, tid string, payload []byte) error {
	msgID := uuid.NewV4().String()
	log.Infof("Sending concept=[%s] uuid=[%s] tid=[%v] msgId=[%v]", conceptType, id, tid, msgID)
	message := producer.Message{
		Headers: buildHeader(msgID, conceptType, tid),
		Body:    string(payload),
	}
	return (*q.producer).SendMessage(msgID, message)
}

func buildHeader(msgID string, conceptType string, tid string) map[string]string {
	return map[string]string{
		"Message-Id":        msgID,
		"Message-Type":      conceptType,
		"Content-Type":      "application/json",
		"X-Request-Id":      tid,
		"Origin-System-Id":  "http://cmdb.ft.com/systems/upp",
		"Message-Timestamp": time.Now().Format(messageTimestampDateFormat),
	}
}
