package kafka

import (
	"log"
	"time"

	"github.com/Shopify/sarama"
)

func NewKafkaConsumer(url string) sarama.Consumer {
	brokers := GetBrokers(url)
	client := <-RetryConnect(brokers, 5*time.Second) // 연결에 성공할 때 까지 반복

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatal(err)
	}

	return consumer
}

// func NewKafkaConsumerGroup() sarama.ConsumerGroup {

// }
