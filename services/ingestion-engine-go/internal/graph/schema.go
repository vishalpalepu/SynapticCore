package graph

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func (w *Writer) InitializeSchema(ctx context.Context) error {

	session := w.Driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

	// Primary approach: APOC (single DB call)
	apocQuery := `
	CALL apoc.cypher.runMany("
	CREATE CONSTRAINT entity_uid IF NOT EXISTS FOR (n:Entity) REQUIRE n.uid IS UNIQUE;
	CREATE CONSTRAINT event_uid IF NOT EXISTS FOR (n:Event) REQUIRE n.uid IS UNIQUE;
	CREATE CONSTRAINT concept_uid IF NOT EXISTS FOR (n:Concept) REQUIRE n.uid IS UNIQUE;
	CREATE CONSTRAINT instruction_uid IF NOT EXISTS FOR (n:Instruction) REQUIRE n.uid IS UNIQUE;
	CREATE CONSTRAINT document_uid IF NOT EXISTS FOR (n:Document) REQUIRE n.uid IS UNIQUE;
	CREATE CONSTRAINT chunk_uid IF NOT EXISTS FOR (n:Chunk) REQUIRE n.uid IS UNIQUE;

	CREATE INDEX entity_name_idx IF NOT EXISTS FOR (n:Entity) ON (n.name);
	CREATE INDEX concept_term_idx IF NOT EXISTS FOR (n:Concept) ON (n.term);
	CREATE INDEX chunk_doc_uid_idx IF NOT EXISTS FOR (n:Chunk) ON (n.doc_uid);
	", {})
	`

	// Try APOC first
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, apocQuery, nil)
		return nil, err
	})

	if err == nil {
		return nil // success with APOC
	}

	// Fallback: no APOC → run in single transaction loop
	commands := []string{
		"CREATE CONSTRAINT entity_uid IF NOT EXISTS FOR (n:Entity) REQUIRE n.uid IS UNIQUE",
		"CREATE CONSTRAINT event_uid IF NOT EXISTS FOR (n:Event) REQUIRE n.uid IS UNIQUE",
		"CREATE CONSTRAINT concept_uid IF NOT EXISTS FOR (n:Concept) REQUIRE n.uid IS UNIQUE",
		"CREATE CONSTRAINT instruction_uid IF NOT EXISTS FOR (n:Instruction) REQUIRE n.uid IS UNIQUE",
		"CREATE CONSTRAINT document_uid IF NOT EXISTS FOR (n:Document) REQUIRE n.uid IS UNIQUE",
		"CREATE CONSTRAINT chunk_uid IF NOT EXISTS FOR (n:Chunk) REQUIRE n.uid IS UNIQUE",

		"CREATE INDEX entity_name_idx IF NOT EXISTS FOR (n:Entity) ON (n.name)",
		"CREATE INDEX concept_term_idx IF NOT EXISTS FOR (n:Concept) ON (n.name)",
		"CREATE INDEX chunk_doc_uid_idx IF NOT EXISTS FOR (n:Chunk) ON (n.doc_uid)",
	}

	_, fallbackErr := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		for _, cmd := range commands {
			if _, err := tx.Run(ctx, cmd, nil); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})

	if fallbackErr != nil {
		return fmt.Errorf("schema initialization failed (apoc + fallback): %w", fallbackErr)
	}

	return nil
}
