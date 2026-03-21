package graph

import (
	"context"
	"fmt"
	"services/ingestion-engine-go/services/ingestion-engine-go/internal/extractor"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Writer struct {
	Driver neo4j.DriverWithContext
}

func (w *Writer) WriteExtraction(
	ctx context.Context,
	docID string,
	chunk extractor.Chunk,
	result *extractor.GraphResult,
) error {

	session := w.Driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		if err := w.writeChunk(ctx, tx, docID, chunk); err != nil {
			return nil, err
		}

		for _, n := range result.Nodes {
			if err := w.mergeNode(ctx, tx, n); err != nil {
				return nil, err
			}
		}

		for _, r := range result.Relationships {
			if err := w.mergeRelationship(ctx, tx, r); err != nil {
				return nil, err
			}
		}

		return nil, nil
	})

	return err
}

func (w *Writer) WriteExtractionOptimized(
	ctx context.Context,
	docID string,
	chunk extractor.Chunk,
	result *extractor.GraphResult,
) error {

	session := w.Driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		if err := w.writeChunk(ctx, tx, docID, chunk); err != nil {
			return nil, err
		}

		// 2. Batch nodes (grouped by label)
		groupedNodes := groupNodesByLabel(result.Nodes)

		for label, nodes := range groupedNodes {
			if err := w.batchMergeNodesByLabel(ctx, tx, label, nodes); err != nil {
				return nil, err
			}
		}

		grouped := groupRelationshipsByType(result.Relationships)

		for relType, rels := range grouped {
			if err := w.batchMergeRelationships(ctx, tx, relType, rels); err != nil {
				return nil, err
			}
		}

		return nil, nil
	})

	return err
}

func (w *Writer) writeChunk(ctx context.Context, tx neo4j.ManagedTransaction, docID string, chunk extractor.Chunk) error {

	query := `
MERGE (d:document {uid:$doc_uid})
SET d.id = $doc_id

MERGE (c:chunk {uid : $chunk_uid})
SET c.text =  $text
	c.index = $idx
	c.parent_uid = $parent_uid
	c.doc_uid = $doc_uid
	c.token_count = $tokens
	c.updated_at = datetime()

MERGE (d)-[:HAS_CHUNK]->(c)
`

	_, err := tx.Run(ctx, query, map[string]any{
		"doc_uid":    extractor.MakeUID("Document", docID),
		"doc_id":     docID,
		"chunk_uid":  chunk.ChunkID,
		"text":       chunk.Text,
		"idx":        chunk.Index,
		"parent_uid": chunk.ParentID,
		"tokens":     chunk.TokenCount,
	})
	if err != nil {
		return err
	}

	if chunk.Index > 0 {
		prevChunkId := fmt.Sprintf("%s-chunk-%d", docID, chunk.Index-1)

		nextQuery := `
MATCH (prev:chunk {uid : $prev_uid})
MATCH (curr:chunk {uid :$curr_uid})
MERGE (prev)-[:NEXT_CHUNK]->(curr)
`

		_, err := tx.Run(ctx, nextQuery, map[string]any{
			"prev_uid": prevChunkId,
			"curr_uid": chunk.ChunkID,
		})

		if err != nil {
			return err
		}
	}

	return nil
}

// this mergeNode does a DB call for each Node but the Kafka will send the data in huge size which will cost a lot of latency
// also not applied the locks aswell
// created new methods for Writer to be able to solve these problems below
func (w *Writer) mergeNode(ctx context.Context, tx neo4j.ManagedTransaction, n extractor.Node) error {
	query := fmt.Sprintf(`
MERGE (x:%s {uid: $uid})
SET x.name = $name,
    x += $props,
    x.source_id = $source_id,
    x.confidence = $confidence,
    x.t_valid = $t_valid,
    x.t_invalid = $t_invalid,
    x.t_ingest = $t_ingest
`, n.Label)

	_, err := tx.Run(ctx, query, map[string]any{
		"uid":        n.UID,
		"name":       n.Name,
		"props":      n.Properties,
		"source_id":  n.Metadata.SourceID,
		"confidence": n.Metadata.Confidence,
		"t_valid":    n.Metadata.TValid,
		"t_invalid":  n.Metadata.TInvalid,
		"t_ingest":   n.Metadata.TIngest,
	})

	if err != nil {
		return err
	}

	return nil
}

// BATCH NODE MERGE (UNWIND + LOCK)
// what it does is based on a single label multiple Nodes will be created ans shown below and x._lock is for atomicity
// to avoid race conditions
func (w *Writer) batchMergeNodesByLabel(ctx context.Context, tx neo4j.ManagedTransaction, label string, nodes []extractor.Node) error {

	query := fmt.Sprintf(`
UNWIND $nodes AS n
MERGE (x:%s {uid: n.uid})
SET x._lock = true
WITH x, n
SET x.name = n.name,
    x += n.properties,
    x.source_id = n.metadata.source_id,
    x.confidence = n.metadata.confidence,
    x.t_valid = n.metadata.t_valid,
    x.t_invalid = n.metadata.t_invalid,
    x.t_ingest = n.metadata.t_ingest
REMOVE x._lock
`, label)

	_, err := tx.Run(ctx, query, map[string]any{
		"nodes": nodes,
	})

	return err
}

// this create relationship one by one which make a lot of DB calls to avoid we can perform batch tarnsactions
func (w *Writer) mergeRelationship(ctx context.Context, tx neo4j.ManagedTransaction, r extractor.Relationship) error {

	query := fmt.Sprintf(`
MATCH (a {uid: $source_uid})
MATCH (b {uid: $target_uid})
MERGE (a)-[rel:%s {uid: $uid}]->(b)
SET rel += $props,
    rel.source_id = $source_id,
    rel.confidence = $confidence,
    rel.t_valid = $t_valid,
    rel.t_invalid = $t_invalid,
    rel.t_ingest = $t_ingest
`, r.Type)

	_, err := tx.Run(ctx, query, map[string]any{
		"uid":        r.UID,
		"source_uid": r.SourceUID,
		"target_uid": r.TargetUID,
		"props":      r.Properties,
		"source_id":  r.Metadata.SourceID,
		"confidence": r.Metadata.Confidence,
		"t_valid":    r.Metadata.TValid,
		"t_invalid":  r.Metadata.TInvalid,
		"t_ingest":   r.Metadata.TIngest,
	})

	if err != nil {
		return err
	}

	return nil
}
func (w *Writer) batchMergeRelationships(ctx context.Context, tx neo4j.ManagedTransaction,relType string ,rels []extractor.Relationship) error{

	query :=fmt.Sprintf( `
UNWIND $rels as r
MATCH (a {uid: $source_uid})
MATCH (b {uid: $target_uid})
MERGE (a)-[rel:%s {uid: r.uid}]->(b)
SET rel += r.properties,
    rel.source_id = r.metadata.source_id,
    rel.confidence = r.metadata.confidence,
    rel.t_valid = r.metadata.t_valid,
    rel.t_invalid = r.metadata.t_invalid,
    rel.t_ingest = r.metadata.t_ingest
REMOVE rel._lock`,relType)

	_, err := tx.Run(ctx, query, map[string]any{
		"rels": rels,
	})

	return err
}


// groups Nodes based on Label for batch transaction
func groupNodesByLabel(nodes []extractor.Node) map[string][]extractor.Node {
	out := make(map[string][]extractor.Node)

	for _, n := range nodes {
		out[n.Label] = append(out[n.Label], n)
	}

	return out
}

// groups relationships based on the types
func groupRelationshipsByType(rels []extractor.Relationship) map[string][]extractor.Relationship {
	out := make(map[string][]extractor.Relationship)

	for _, r := range rels {
		out[r.Type] = append(out[r.Type], r)
	}

	return out
}
