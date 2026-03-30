package kafka

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Brokers            []string
	RawTopic           string
	ExtractedTopic     string
	ConsumerGroupRaw   string
	ConsumerGroupGraph string
	Dialer             *kafka.Dialer
}

func getEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("missing required env variable: %s", key)
	}
	return val
}

func LoadConfig() *Config {
	brokers := strings.Split(getEnv("KAFKA_BROKERS"), ",")

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	return &Config{
		Brokers:            brokers,
		RawTopic:           getEnv("RAW_TOPIC"),
		ExtractedTopic:     getEnv("EXTRACTED_TOPIC"),
		ConsumerGroupRaw:   getEnv("RAW_CONSUMER_GROUP"),
		ConsumerGroupGraph: getEnv("GRAPH_CONSUMER_GROUP"),
		Dialer:             dialer,
	}
}
