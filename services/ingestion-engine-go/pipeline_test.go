package pipeline_test

import (
	"context"
	"fmt"
	"ingestion-engine-go/internal/extractor"
	"ingestion-engine-go/internal/graph"
	"ingestion-engine-go/internal/limiter"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func TestFullPipeline(t *testing.T) {
	ctx := context.Background()

	// ========================
	// 1. Load Ontology
	// ========================
	fullPath := "../../shared/docs/onthology-v1.yaml"
	abs, _ := filepath.Abs(fullPath)
	fmt.Println("ABS PATH:", abs)
	schema, err := extractor.LoadOnthology(abs)
	if err != nil {
		t.Fatalf("ontology load failed: %v", err)
	}

	// err = godotenv.Load()
	// if err != nil {
	// 	log.Println("Warning: .env file not found, falling back to system env")
	// }

	err = godotenv.Load()
	if err != nil {
		t.Fatalf("failed to load .env: %v", err)
	}

	// ========================
	// 2. LLM Client (GROK)
	// ========================
	client := &extractor.HTTPLLMClient{
		BaseURL: os.Getenv("LLM_BASE_URL"),
		APIKey:  os.Getenv("LLM_API_KEY"),
		Model:   os.Getenv("LLM_MODEL"),
	}

	transformer := &extractor.Transformer{
		Schema:  schema,
		Client:  client,
		Limiter: limiter.NewTokenBucket(5, time.Hour.Seconds()),
	}

	// ========================
	// 3. Neo4j
	// ========================
	uri := os.Getenv("NEO4J_URI")
	if uri == "" {
		uri = "bolt://localhost:7687"
	}
	user := os.Getenv("NEO4J_USERNAME")
	if user == "" {
		user = "neo4j"
	}
	pass := os.Getenv("NEO4J_PASSWORD")
	driver, err := neo4j.NewDriverWithContext(
		uri,
		neo4j.BasicAuth(
			user,
			pass,
			"",
		),
	)
	if err != nil {
		t.Fatalf("neo4j driver failed: %v", err)
	}
	defer driver.Close(ctx)

	writer := &graph.Writer{
		Driver: driver,
		Logger: log.New(os.Stdout, "[TEST] ", log.LstdFlags),
	}

	// ========================
	// 4. Input Document
	// ========================
	docID := "test-doc-001"
	text := `
A senior cardiologist at a metropolitan hospital had been studying patterns in post-surgical recovery among patients with chronic heart conditions. Over several months, she observed that individuals who adhered to structured rehabilitation programs tended to show more stable progress compared to those who did not.`

	// ========================
	// 5. Chunking
	// ========================
	chunks := extractor.ChuckText(docID, text, 20, 0.1)

	pkg := &extractor.DocumentPackage{
		DocID:  docID,
		Chunks: chunks,
	}

	// ========================
	// 6. Extraction
	// ========================
	for _, chunk := range chunks {
		res, err := transformer.ExtractChunkOptimized(ctx, chunk)
		if err != nil {
			t.Fatalf("llm extraction failed: %v", err)
		}

		pkg.Nodes = append(pkg.Nodes, res.Nodes...)
		pkg.Relationships = append(pkg.Relationships, res.Relationships...)
	}

	// ========================
	// 7. Neo4j Write
	// ========================
	err = writer.WriteDocumentPackage(ctx, pkg)
	if err != nil {
		t.Fatalf("neo4j write failed: %v", err)
	}

	// ========================
	// 8. Verification
	// ========================
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := `
MATCH (d:Document {id: $docID})-->(c:Chunk)
RETURN count(c) as chunkCount
`
		res, err := tx.Run(ctx, query, map[string]any{"docID": docID})
		if err != nil {
			return nil, err
		}
		return res.Single(ctx)
	})

	if err != nil {
		t.Fatalf("verification query failed: %v", err)
	}

	if result == nil {
		t.Fatalf("no data found in neo4j")
	}
}
