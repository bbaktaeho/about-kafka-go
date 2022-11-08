package kafka

import (
	"errors"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bbaktaeho/about-kafka-go/topics"
)

// NewSyncProducer 함수는 sarama의 동기 프로듀서를 생성합니다.
func NewSyncProducer(url string) sarama.SyncProducer {
	brokers := GetBrokers(url)
	client := <-RetryConnect(brokers, 5*time.Second) // 연결에 성공할 때 까지 반복
	createTopics(brokers, client.Config())

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatal("[NewSyncProducer]", err)
	}

	return producer
}

// NewAsyncProducer 함수는 sarama의 비동기 프로듀서를 생성합니다.
func NewAsyncProducer(url string) sarama.AsyncProducer {
	brokers := GetBrokers(url)
	client := <-RetryConnect(brokers, 5*time.Second) // 연결에 성공할 때 까지 반복
	createTopics(brokers, client.Config())

	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		log.Fatal("[NewAsyncProducer]", err)
	}

	return producer
}

// createTopics 함수는 topic을 생성합니다.
func createTopics(brokers []string, config *sarama.Config) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatal("[createTopics]", err)
	}
	defer admin.Close()

	topics := []string{
		topics.A,
		topics.B,
		topics.C,
		topics.D,
		topics.E,
	}

	for _, topic := range topics {
		err = admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err != nil {
			if !errors.Is(err, sarama.ErrTopicAlreadyExists) {
				log.Fatal("[createTopics]", err)
			}
		}
	}
}
