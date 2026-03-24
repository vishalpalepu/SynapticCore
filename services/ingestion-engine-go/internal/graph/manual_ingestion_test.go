package graph_test

import (
	"context"
	"ingestion-engine-go/internal/extractor"
	"ingestion-engine-go/internal/graph"
	"log"
	"os"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func TestManualIngestion(t *testing.T) {
	ctx := context.Background()

	driver, err := neo4j.NewDriverWithContext(
		"neo4j://localhost:7687",
		neo4j.BasicAuth("neo4j", "your_secure_password", ""),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close(ctx)
	logger := log.New(os.Stdout, "[INGEST] ", log.LstdFlags)
	writer := &graph.Writer{Driver: driver, Logger: logger}

	// ---- Chunk ----
	chunk := extractor.Chunk{
		DocID:      "doc1",
		ChunkID:    "doc1-chunk-0",
		ParentID:   "doc1-parent",
		Index:      0,
		Text:       "John Doe works at OpenAI.",
		TokenCount: 10,
	}

	now := time.Now().UTC()

	// ---- Nodes ----
	personName := extractor.CanonicalName("John Doe")
	companyName := extractor.CanonicalName("OpenAI")

	personLabel := extractor.SanitizeLabel("Entity")
	companyLabel := extractor.SanitizeLabel("Entity")

	personUID := extractor.MakeUID(personLabel, personName)
	companyUID := extractor.MakeUID(companyLabel, companyName)

	nodes := []extractor.Node{
		{
			Name:  personName,
			Label: personLabel,
			UID:   personUID,
			Properties: map[string]any{
				"canonical_name": personName,
			},
			Metadata: extractor.Metadata{
				UID:        personUID,
				SourceID:   chunk.ChunkID,
				Confidence: 1.0,
				TIngest:    now,
			},
		},
		{
			Name:  companyName,
			Label: companyLabel,
			UID:   companyUID,
			Properties: map[string]any{
				"canonical_name": companyName,
			},
			Metadata: extractor.Metadata{
				UID:        companyUID,
				SourceID:   chunk.ChunkID,
				Confidence: 1.0,
				TIngest:    now,
			},
		},
	}

	// ---- Relationship (valid per ontology) ----
	relType := extractor.SanitizeLabel("ASSOCIATED_WITH")
	relUID := extractor.MakeUID("relationship", personUID+"|"+relType+"|"+companyUID)

	rels := []extractor.Relationship{
		{
			Type:        relType,
			SourceName:  personName,
			TargetName:  companyName,
			SourceUID:   personUID,
			TargetUID:   companyUID,
			SourceLabel: personLabel,
			TargetLabel: companyLabel,
			UID:         relUID,
			Metadata: extractor.Metadata{
				UID:        relUID,
				SourceID:   chunk.ChunkID,
				Confidence: 1.0,
				TIngest:    now,
			},
		},
	}

	graphResult := &extractor.GraphResult{
		Nodes:         nodes,
		Relationships: rels,
	}

	// ---- Write (FIRST RUN) ----
	if err := writer.WriteExtractionOptimized(ctx, "doc1", chunk, graphResult); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// ---- Write (SECOND RUN - IDEMPOTENCY CHECK) ----
	if err := writer.WriteExtractionOptimized(ctx, "doc1", chunk, graphResult); err != nil {
		t.Fatalf("Second write failed: %v", err)
	}

	// ---- Verify ----
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	verifyQuery := `
	MATCH (p:Entity {uid: $p_uid})-[r]->(c:Entity {uid: $c_uid})
	RETURN p.name AS name,
	       type(r) AS rel_type,
	       r.uid AS rel_uid,
	       p._lock AS lock
	`

	res, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, verifyQuery, map[string]any{
			"p_uid": personUID,
			"c_uid": companyUID,
		})
		if err != nil {
			return nil, err
		}
		return result.Single(ctx)
	})

	if err != nil {
		t.Fatalf("Verification query failed: %v", err)
	}

	record := res.(*neo4j.Record)

	// ---- Assertions ----

	// Node name
	if name, _ := record.Get("name"); name != "John Doe" {
		t.Errorf("Expected John Doe, got %v", name)
	}

	// Relationship type
	if relTypeVal, _ := record.Get("rel_type"); relTypeVal != relType {
		t.Errorf("Expected rel type %s, got %v", relType, relTypeVal)
	}

	// Relationship UID
	if relUIDVal, _ := record.Get("rel_uid"); relUIDVal != relUID {
		t.Errorf("Expected rel UID %s, got %v", relUID, relUIDVal)
	}

	// Lock must be removed
	if lock, _ := record.Get("lock"); lock != nil {
		t.Errorf("Architectural Failure: _lock property was not removed from node!")
	}
}
