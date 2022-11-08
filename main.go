package main

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bbaktaeho/about-kafka-go/kafka"
	"github.com/bbaktaeho/about-kafka-go/topics"
)

const (
	KAFKA_URL = "127.0.0.1:9092"
)

func runSyncPub(topic string, timestamp time.Duration) {
	producer := kafka.NewSyncProducer(KAFKA_URL)

	go func() {
		defer producer.Close()
		for {
			value := fmt.Sprintf("[%s] %s", topic, strconv.Itoa(int(time.Now().UnixMilli())))
			msg := &sarama.ProducerMessage{Topic: topics.A, Value: sarama.StringEncoder(value)}
			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				log.Println("[runSyncPub] error", err)
				continue
			}

			log.Printf("[runSyncPub] topic: %s, partition: %d, offset: %d\n", msg.Topic, partition, offset)
			time.Sleep(timestamp)
		}
	}()

}

func runAsyncPub(topic string, timestamp time.Duration) {
	producer := kafka.NewAsyncProducer(KAFKA_URL)

	go func() {
		for {
			select {
			case result := <-producer.Successes():
				log.Printf("[runAsyncPub] topic: %s, partition: %d, offset: %d\n", result.Topic, result.Partition, result.Offset)
			case result := <-producer.Errors():
				if errors.Is(result.Err, sarama.ErrClosedClient) || errors.Is(result.Err, sarama.ErrNotConnected) {
					log.Println("[runAsyncPub] channel close")
					return
				}
				log.Println("[runAsyncPub] error", result.Err)
			}
		}
	}()

	go func() {
		defer producer.AsyncClose()
		for {
			value := fmt.Sprintf("[%s] %s", topic, strconv.Itoa(int(time.Now().UnixMilli())))
			msg := &sarama.ProducerMessage{Topic: topics.B, Value: sarama.StringEncoder(value)}
			producer.Input() <- msg
			time.Sleep(timestamp)
		}
	}()
}

func runSingleSub(topic string, partition int32) {
	consumer := kafka.NewConsumer(KAFKA_URL)

	consumePartition, err := consumer.ConsumePartition(topics.A, partition, sarama.OffsetOldest)
	if err != nil {
		log.Println("[runSingleSub] error", err)
		return
	}

	go func() {
		defer consumer.Close()
		for {
			select {
			case msg := <-consumePartition.Messages():
				topic := msg.Topic
				partition := msg.Partition
				offset := msg.Offset
				value := string(msg.Value)
				log.Printf("[runSingleSub] topic: %v, partition: %d, offset: %d, value: %v\n", topic, partition, offset, value)
			case err := <-consumePartition.Errors():
				log.Println("[runSingleSub] error", err)
			}
		}
	}()
}

func runOffsetManagerSub(groupId string, topic string, partition int32) {
	consumer, offsetManager := kafka.NewOffsetManager(KAFKA_URL, groupId)

	partitionOffsetManager, err := offsetManager.ManagePartition(topics.A, partition)
	if err != nil {
		log.Println("[runOffsetManagerSub] error", err)
		return
	}

	nextOffset, _ := partitionOffsetManager.NextOffset()
	consumePartition, err := consumer.ConsumePartition(topic, partition, nextOffset)
	if err != nil {
		log.Println("[runOffsetManagerSub] error", err)
	}

	go func() {
		defer consumer.Close()
		defer offsetManager.Close()
		for {
			select {
			case msg := <-consumePartition.Messages():
				topic := msg.Topic
				partition := msg.Partition
				offset := msg.Offset
				value := string(msg.Value)
				log.Printf("[runOffsetManagerSub] topic: %v, partition: %d, offset: %d, value: %v\n", topic, partition, offset, value)

				// autu commit이 비활성화라면 수동 실행
				// offsetManager.Commit()
			case err := <-consumePartition.Errors():
				log.Println("[runOffsetManagerSub] error", err)
			}
		}
	}()
}

func main() {
	var topicAPartition int32 = 0
	groupId := "leo"

	runSyncPub(topics.A, 5*time.Second)

	runSingleSub(topics.B, topicAPartition)
	runOffsetManagerSub(groupId, topics.A, topicAPartition)

	time.Sleep(time.Hour)
}
