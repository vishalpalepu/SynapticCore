package graph_test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"ingestion-engine-go/internal/extractor"
	"ingestion-engine-go/internal/graph"

	"github.com/joho/godotenv"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func TestDocumentPackageIngestion(t *testing.T) {
	ctx := context.Background()

	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: .env file not found, falling back to system env")
	}

	// 1. Infrastructure Setup
	uri := os.Getenv("NEO4J_URI")
	user := os.Getenv("NEO4J_USERNAME")
	pass := os.Getenv("NEO4J_PASSWORD")

	if uri == "" {
		uri = "bolt://localhost:7687"
	}
	if user == "" {
		user = "neo4j"
	}

	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(user, pass, ""))
	if err != nil {
		t.Fatalf("Failed to connect to Neo4j: %v", err)
	}
	defer driver.Close(ctx)

	writer := &graph.Writer{Driver: driver, Logger: log.New(os.Stdout, " ", log.LstdFlags)}

	// 2. Build Mock Document Package
	docID := "test-doc-999"
	now := time.Now().UTC()

	// Lexical Chunks
	chunks := []extractor.Chunk{
		{ChunkID: docID + "-c0", Text: "Alice works at OpenAI.", Index: 0, DocID: docID},
		{ChunkID: docID + "-c1", Text: "OpenAI is in San Francisco.", Index: 1, DocID: docID},
	}

	// Semantic Nodes
	aliceUID := extractor.MakeUID("Entity", "Alice")
	openaiUID := extractor.MakeUID("Entity", "OpenAI")
	sfUID := extractor.MakeUID("Entity", "San Francisco")

	nodes := []extractor.Node{
		{UID: aliceUID, Name: "Alice", Label: "Entity", Metadata: extractor.Metadata{SourceID: chunks[0].ChunkID, Confidence: 1.0, TIngest: now}},
		{UID: openaiUID, Name: "OpenAI", Label: "Entity", Metadata: extractor.Metadata{SourceID: chunks[0].ChunkID, Confidence: 1.0, TIngest: now}},
		{UID: sfUID, Name: "San Francisco", Label: "Entity", Metadata: extractor.Metadata{SourceID: chunks[0].ChunkID, Confidence: 1.0, TIngest: now}},
	}

	// Semantic Relationships
	rels := []extractor.Relationship{
		{
			UID:  extractor.MakeUID("relationship", aliceUID+"|WORKS_AT|"+openaiUID),
			Type: "WORKS_AT", SourceUID: aliceUID, SourceLabel: "Entity", TargetUID: openaiUID, TargetLabel: "Entity",
			Metadata: extractor.Metadata{SourceID: chunks[0].ChunkID, Confidence: 1.0, TIngest: now},
		},
		{
			UID:  extractor.MakeUID("relationship", openaiUID+"|LOCATED_IN|"+sfUID),
			Type: "LOCATED_IN", SourceUID: openaiUID, SourceLabel: "Entity", TargetUID: sfUID, TargetLabel: "Entity",
			Metadata: extractor.Metadata{SourceID: chunks[1].ChunkID, Confidence: 1.0, TIngest: now},
		},
	}

	pkg := extractor.DocumentPackage{
		DocID:         docID,
		Chunks:        chunks,
		Nodes:         nodes,
		Relationships: rels,
		TIngest:       now,
	}

	// 3. Execute Atomic Write
	if err := writer.WriteDocumentPackage(ctx, pkg); err != nil {
		t.Fatalf("DocumentPackage write failed: %v", err)
	}

	// 4. State Verification Phase
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	verifyQuery := `
	MATCH (d:Document {id: $doc_id})-->(c1:Chunk {index: 0})-->(c2:Chunk {index: 1})
	MATCH (a:Entity {uid: $a_uid})-->(o:Entity {uid: $o_uid})-->(s:Entity {uid: $s_uid})
	RETURN a.name AS name, a._lock AS lock
	`
	res, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, verifyQuery, map[string]any{
			"doc_id": docID, "a_uid": aliceUID, "o_uid": openaiUID, "s_uid": sfUID,
		})
		if err != nil {
			return nil, err
		}
		return result.Single(ctx)
	})

	if err != nil {
		t.Fatalf("Database verification failed. The graph structure is incomplete or broken: %v", err)
	}

	record := res.(*neo4j.Record)

	// Assert Identity
	if name, _ := record.Get("name"); name != "Alice" {
		t.Errorf("Expected Alice, got %v", name)
	}

	// CRITICAL: Verify lock property was removed by the REMOVE x._lock clause
	if lock, _ := record.Get("lock"); lock != nil {
		t.Errorf("Architectural Failure: _lock property was not removed from the node. Future concurrent writes will hang.")
	}
}
