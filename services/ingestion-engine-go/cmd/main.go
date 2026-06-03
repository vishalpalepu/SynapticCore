package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"ingestion-engine-go/internal/extractor"
	"ingestion-engine-go/internal/graph"
	"ingestion-engine-go/internal/kafka"
	"ingestion-engine-go/internal/limiter"

	kafkago "github.com/segmentio/kafka-go" // kafkago is used to avoid naming conflict with "kafka-go" package in transformer.go

	"github.com/joho/godotenv"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func main() {
	_ = godotenv.Load() // Load .env file if it exists

	logger := log.New(os.Stdout, "ingestion-engine: ", log.LstdFlags|log.Lshortfile)

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	rawTopic := os.Getenv("KAFKA_RAW_TOPIC")
	if rawTopic == "" {
		rawTopic = "raw-documents"
	}

	tripleTopic := os.Getenv("KAFKA_TRIPLE_TOPIC")
	if tripleTopic == "" {
		tripleTopic = "extracted-triples"
	}

	groupID := os.Getenv("KAFKA_GROUP_ID")
	if groupID == "" {
		groupID = "synaptic-memory-grou"
	}

	neo4jURI := os.Getenv("NEO4J_URI")
	if neo4jURI == "" {
		neo4jURI = "neo4j://localhost:7687"
	}

	neo4jUsername := os.Getenv("NEO4J_USERNAME")
	if neo4jUsername == "" {
		neo4jUsername = "neo4j"
	}

	neo4jPassword := os.Getenv("NEO4J_PASSWORD")
	if neo4jPassword == "" {
		neo4jPassword = "password"
	}

	onthologyPath := os.Getenv("ONTOLOGY_PATH")
	if onthologyPath == "" {
		onthologyPath = "../../shared/docs/ontology-v1.yaml"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM) //stops when ctrl+c (os.Interrupt) or kill command (syscall.SIGTERM) is sent
	defer stop()

	driver, err := neo4j.NewDriverWithContext(neo4jURI, neo4j.BasicAuth(neo4jUsername, neo4jPassword, ""))

	if err != nil {
		logger.Fatalf("Neo4j Connection Failed: %v", err)
	}
	driver.Close(ctx)

	writer := &graph.Writer{
		Driver: driver,
		Logger: logger,
	}
	logger.Println("Enforcing Database Constraints and Indexes...")

	if err := writer.InitializeSchema(ctx); err != nil {
		logger.Fatalf("Schema Initialization Failed: %v", err)
	}
	logger.Println("Database Schema is ONLINE and hardened.")

	schema, err := extractor.LoadOnthology(onthologyPath)
	if err != nil {
		logger.Fatalf("Ontology Load Failed: %v", err)
	}

	llmClient := &extractor.HTTPLLMClient{
		BaseURL: os.Getenv("LLM_BASE_URL"),
		APIKey:  os.Getenv("LLM_API_KEY"),
		Model:   os.Getenv("LLM_MODEL"),
	}
	// Token Bucket: Throttling client calls to prevent 429 API bans
	rateLimter := limiter.NewTokenBucket(5, 3600) // 5 calls per hour

	transformer := &extractor.Transformer{
		Schema:  schema,
		Client:  llmClient,
		Limiter: rateLimter,
	}

	rawReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        []string{brokers},
		GroupID:        groupID,
		Topic:          rawTopic,
		CommitInterval: 0, // Disable auto-commit to manage offsets manually after processing
	})
	defer rawReader.Close()

	tripleProducer := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers),
		Topic:        tripleTopic,
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll, // Require all replicas to acknowledge write
	}
	defer tripleProducer.Close()

	tripleReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        []string{brokers},
		GroupID:        groupID,
		Topic:          tripleTopic,
		CommitInterval: 0, // Disable auto-commit to manage offsets manually after processing
	})
	defer tripleReader.Close()

	extractorWorker := &kafka.ExtractorWorker{
		Reader:         rawReader,
		TripleProducer: tripleProducer,
		Transformer:    transformer,
		Logger:         logger,
		WorkerCount:    4, // Number of concurrent workers for extraction
	}

	graphWorker := &kafka.GraphWorker{
		Reader:      tripleReader,
		Logger:      logger,
		GraphWriter: writer,
		WorkerCount: 3,
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		extractorWorker.Start(ctx) // Blocks internally until ctx is cancelled
	}()

	go func() {
		defer wg.Done()
		graphWorker.Start(ctx) // Blocks internally until ctx is cancelled
	}()

	logger.Println("SynapticCore pipeline is operational. Listening for streaming events...")

	<-ctx.Done()
	logger.Println("SIGINT/SIGTERM detected. Initiating graceful shutdown sequence...")

	rawReader.Close()
	tripleReader.Close()

	logger.Println("Awaiting active worker tasks to finish processing and drain channels...")
	wg.Wait()

	logger.Println("SynapticCore Ingestion Engine has successfully shutdown. No offsets leaked.")
}
