package kafka

import (
	"log"
	"time"

	"github.com/Shopify/sarama"
)

func NewConsumer(url string) sarama.Consumer {
	brokers := GetBrokers(url)
	client := <-RetryConnect(brokers, 5*time.Second) // 연결에 성공할 때 까지 반복

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatal("[NewConsumer]", err)
	}

	return consumer
}

func NewOffsetManager(url string, groupId string) (sarama.Consumer, sarama.OffsetManager) {
	brokers := GetBrokers(url)
	client := <-RetryConnect(brokers, 5*time.Second) // 연결에 성공할 때 까지 반복

	config := client.Config()
	config.Consumer.Offsets.AutoCommit.Enable = true
	// default 1s
	// config.Consumer.Offsets.AutoCommit.Interval

	offsetManager, err := sarama.NewOffsetManagerFromClient(groupId, client)
	if err != nil {
		log.Fatal("[NewOffsetManager]", err)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatal("[NewOffsetManager]", err)
	}

	return consumer, offsetManager
}

// func NewKafkaConsumerGroup() sarama.ConsumerGroup {

// }
