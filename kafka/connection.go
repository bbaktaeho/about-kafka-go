package kafka

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

func GetBrokers(url string) []string {
	if url != "" {
		return strings.Split(url, ",")
	}

	return nil
}

// retryConnect 함수는 kafka broker와 연결이 성공될 때까지 재시도합니다.
func RetryConnect(brokers []string, retryInterval time.Duration) chan sarama.Client {
	result := make(chan sarama.Client)

	go func() {
		defer close(result)
		for {
			config := sarama.NewConfig()
			config.Producer.Return.Successes = true
			config.Producer.Return.Errors = true
			conn, err := sarama.NewClient(brokers, config)
			if err == nil {
				result <- conn
				return
			}

			msg := fmt.Sprintf("Kafka connection failed with error (retrying in %s)", retryInterval.String())
			log.Println(msg)
			time.Sleep(retryInterval)
		}
	}()

	return result
}
