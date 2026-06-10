package kafka_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"ingestion-engine-go/internal/extractor"
	"ingestion-engine-go/internal/graph"
	"ingestion-engine-go/internal/kafka"
	"ingestion-engine-go/internal/limiter"

	"github.com/joho/godotenv"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	kafkago "github.com/segmentio/kafka-go"
)

type MockLLMClient struct{}

// a Mock for LLM extraction where the normal text -> Neo4j conversion takes place
func (m *MockLLMClient) Extract(ctx context.Context, systemPrompt, userPrompt string) (string, error) {
	// Returns valid, schema-compliant JSON mimicking our ontology extraction
	return `{
		"nodes": [
			{"name": "Alice", "label": "Entity", "metadata": {"confidence": 1.0}},
			{"name": "OpenAI", "label": "Entity", "metadata": {"confidence": 1.0}}
		],
		"relationships":
	}`, nil
}

func TestEndToEndPipeline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	_ = godotenv.Load("../../.env")

	brokers := "localhost:9092"
	rawTopic := "raw-documents"
	tripleTopic := "extracted-triples"
	groupID := "test-pipeline-group"

	neo4jURI := os.Getenv("NEO4J_URI")
	if neo4jURI == "" {
		neo4jURI = "bolt://localhost:7687"
	}
	neo4jUser := os.Getenv("NEO4J_USERNAME")
	if neo4jUser == "" {
		neo4jUser = "neo4j"
	}
	neo4jPass := os.Getenv("NEO4J_PASSWORD")

	logger := log.New(os.Stdout, " ", log.LstdFlags)

	// 1. Setup Neo4j and Initialize Schema
	driver, err := neo4j.NewDriverWithContext(neo4jURI, neo4j.BasicAuth(neo4jUser, neo4jPass, ""))
	if err != nil {
		t.Fatalf("Neo4j driver initialization failed: %v", err)
	}
	defer driver.Close(ctx)

	writer := &graph.Writer{Driver: driver, Logger: logger}
	_ = writer.InitializeSchema(ctx) // Idempotent check

	// 2. Setup Mock Extraction Pipeline
	schema, err := extractor.LoadOnthology("../../../../shared/docs/onthology-v1.yaml")
	if err != nil {
		t.Fatalf("Failed to load ontology schema: %v", err)
	}

	transformer := &extractor.Transformer{
		Schema:  schema,
		Client:  &MockLLMClient{},
		Limiter: limiter.NewTokenBucket(10, 10.0),
	}

	// 3. Setup Readers & Writers
	rawReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: []string{brokers}, GroupID: groupID, Topic: rawTopic, CommitInterval: 0,
	})
	defer rawReader.Close()

	tripleProducer := &kafkago.Writer{
		Addr: kafkago.TCP(brokers), Topic: tripleTopic, RequiredAcks: kafkago.RequireAll,
	}
	defer tripleProducer.Close()

	tripleReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: []string{brokers}, GroupID: groupID, Topic: tripleTopic, CommitInterval: 0,
	})
	defer tripleReader.Close()

	// External Producer to simulate Raw File Ingress
	rawProducer := &kafkago.Writer{
		Addr: kafkago.TCP(brokers), Topic: rawTopic, RequiredAcks: kafkago.RequireAll,
	}
	defer rawProducer.Close()

	// 4. Start Stage 1 and Stage 2 Background Workers
	extractorWorker := &kafka.ExtractorWorker{
		Reader:         rawReader,
		TripleProducer: tripleProducer,
		Transformer:    transformer,
		Logger:         logger,
		WorkerCount:    1,
	}

	graphWorker := &kafka.GraphWorker{
		Reader:      tripleReader,
		Logger:      logger,
		GraphWriter: writer,
		WorkerCount: 1,
	}

	go extractorWorker.Start(ctx)
	go graphWorker.Start(ctx)

	// Ingestion the document into the raw topic (Topic 1) to trigger the pipeline
	testDocID := fmt.Sprintf("pipeline-doc-%d", time.Now().UnixNano())
	rawDocText := "Alice is an engineer who was hired by OpenAI."

	logger.Printf("Processing seed document %s into %s...", testDocID, rawTopic)
	err = rawProducer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(testDocID),
		Value: []byte(rawDocText),
	})
	if err != nil {
		logger.Fatalf("Failed to write seed document to Kafka: %v", err)
	}

	logger.Printf("Awaiting async extraction and graph insertion for document %s...", testDocID)

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer driver.Close(ctx)

	// this comes after the topic 1 is consumed and  computed to get the llm json response which is then converted to a perfect valid nodes
	// and relationships
	aliceUID := extractor.MakeUID("Entity", "Alice")
	openaiUID := extractor.MakeUID("Entity", "OpenAI")

	verifyQuery := `
	MATCH (d:Document {id: $doc_id})-->(c:Chunk)
	MATCH (a:Entity {uid: $a_uid})-->(o:Entity {uid: $o_uid})
	RETURN a.name AS name, type(r) AS relType, a._lock AS lock
	`
	success := false
	for i := 0; i < 15; i++ { // Poll for up to 15 seconds
		time.Sleep(1 * time.Second)
		res, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			result, err := tx.Run(ctx, verifyQuery, map[string]any{
				"doc_id": testDocID, "a_uid": aliceUID, "o_uid": openaiUID,
			})
			if err != nil {
				return nil, err
			}
			return result.Single(ctx)
		})

		if err == nil && res != nil {
			record := res.(*neo4j.Record)
			name, _ := record.Get("name")
			lock, _ := record.Get("lock")

			if name == "Alice" && lock == nil {
				success = true
				break
			}
		}
	}
	if !success {
		t.Fatalf("Pipeline Assertion Failed: Graph structure was not committed or locks were not released properly.")
	}

	logger.Println("End-to-End Pipeline test PASSED cleanly. State validated in Neo4j.")

}
