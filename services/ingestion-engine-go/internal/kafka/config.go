package kafka

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Brokers         []string
	IngestionTopic  string
	ConsumerGroupID string
	Dialer          *kafka.Dialer
}

func getEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("Environment variable %s is not set", key)
	}
	return val
}

func LoadConfig() *Config {
	brokersEnv := getEnv("KAFKA_BROKERS")
	topic := getEnv("INGESTION_TOPIC")
	groupID := getEnv("CONSUMER_GROUP_ID")

	brokers := strings.Split(brokersEnv, ",")

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	cfg := &Config{
		Brokers:         brokers,
		IngestionTopic:  topic,
		ConsumerGroupID: groupID,
		Dialer:          dialer,
	}

	return cfg
}
