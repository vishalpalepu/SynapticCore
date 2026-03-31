package graph

import (
	"context"
	"fmt"
	"ingestion-engine-go/internal/extractor"
	"log"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Writer struct {
	Driver neo4j.DriverWithContext
	Logger *log.Logger
}

// OLD APPROACH OF USING ONLY THE SINGLE KAFKA TOPIC WHERE THE NEO4J CALLS HAPPEN FOR EACH CHUNK
// WE MODIFY IT SO THAT THE KAFKA TOPIC WILL SEND THE DATA IN BATCHES (LIKE 10 CHUNKS WITH THEIR NODES AND RELS)
// AND THEN WE WILL PERFORM BATCH MERGE USING UNWIND IN NEO4J WHICH IS MUCH FASTER THAN SINGLE MERGE

// NEW APPROACH IS TO SEND THE DATA IN BATCHES (LIKE 10 CHUNKS WITH THEIR NODES AND RELS)

func (w *Writer) WriteDocumentPackage(ctx context.Context, pkg extractor.DocumentPackage) error {
	session := w.Driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		start := time.Now()
		if w.Logger != nil {
			w.Logger.Printf("START ingestion doc=%s chunks=%d nodes=%d rels=%d",
				pkg.DocID,
				len(pkg.Chunks),
				len(pkg.Nodes),
				len(pkg.Relationships),
			)
		}

		if err := w.batchWriteChunks(ctx, tx, pkg.DocID, pkg.Chunks); err != nil {
			if w.Logger != nil {
				w.Logger.Printf("Error writing chunks doc=%s err=%v", pkg.DocID, err)
			}
			return nil, err
		}

		groupedNodes := groupNodesByLabel(pkg.Nodes)

		for label, nodes := range groupedNodes {
			if w.Logger != nil {
				w.Logger.Printf("Batch Nodes label=%s size=%d", label, len(nodes))
			}
			if err := w.batchMergeNodesByLabel(ctx, tx, label, nodes); err != nil {
				if w.Logger != nil {
					w.Logger.Printf("Error merging nodes label=%s size=%d", label, len(nodes))
				}
				return nil, err
			}
		}

		groupedRels := groupRelationshipsAdvanced(pkg.Relationships)

		for key, rels := range groupedRels {
			if w.Logger != nil {
				w.Logger.Printf("Batch Rels type=%s size=%d", key.Type, len(rels))
			}

			if err := w.batchMergeRelationshipsWithLabels(ctx, tx, key, rels); err != nil {
				if w.Logger != nil {
					w.Logger.Printf("Error merging relationships type=%s size=%d", key.Type, len(rels))
				}
				return nil, err
			}
		}

		if w.Logger != nil {
			w.Logger.Printf("END ingestion doc=%s duration=%s",
				pkg.DocID,
				time.Since(start),
			)
		}

		return nil, nil
	})
	return err
}

func (w *Writer) batchWriteChunks(ctx context.Context, tx neo4j.ManagedTransaction, docID string, chunks []extractor.Chunk) error {

	query := `
MERGE (d:Document {uid:$doc_uid})
SET d.id = $doc_id

WITH d
UNWIND $chunks AS c

MERGE (chunk:Chunk {uid: c.chunk_id})
SET chunk.text = c.text,
	chunk.index = c.index,
	chunk.parent_uid = c.parent_uid,
	chunk.doc_uid = c.doc_uid,
	chunk.token_count = c.token_count,
	chunk.updated_at = datetime()
	
MERGE (d)-[:HAS_CHUNK]->(chunk)
`
	_, err := tx.Run(ctx, query, map[string]any{
		"doc_uid": extractor.MakeUID("Document", docID),
		"doc_id":  docID,
		"chunks":  chunksToMap(docID, chunks),
	})

	if err != nil {
		return err
	}

	linkQuery := `
UNWIND $links AS l 
MATCH (prev:Chunk {uid : l.from})
MATCH (curr:Chunk {uid : l.to})
MERGE (prev)-[:NEXT_CHUNK]->(curr)
`
	_, err = tx.Run(ctx, linkQuery, map[string]any{
		"links": batchChunkLinks(chunks),
	})

	return err
}

func chunksToMap(docID string, chunks []extractor.Chunk) []map[string]any {
	out := make([]map[string]any, 0, len(chunks))

	for _, c := range chunks {
		out = append(out, map[string]any{
			"chunk_id":    c.ChunkID,
			"text":        c.Text,
			"index":       c.Index,
			"parent_uid":  c.ParentID,
			"doc_uid":     extractor.MakeUID("Document", docID),
			"token_count": c.TokenCount,
		})
	}
	return out
}

func batchChunkLinks(chunks []extractor.Chunk) []map[string]any {
	out := make([]map[string]any, 0, len(chunks)-1)

	for i := 1; i < len(chunks); i++ {
		out = append(out, map[string]any{
			"from": chunks[i-1].ChunkID,
			"to":   chunks[i].ChunkID,
		})
	}
	return out
}

// OLD APPROACH
// WRAPPER FOR HAVING A ROLLBACK IN CASE OF ERROR
// TO SUPPORT TRANSACTIONAL INTEGRITY (to incase anyone fails everything is rolled back)
func (w *Writer) WriteExtractionOptimized(ctx context.Context, docID string, chunk extractor.Chunk, result *extractor.GraphResult) error {

	session := w.Driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		start := time.Now()
		if w.Logger != nil {
			w.Logger.Printf("START ingestion doc=%s chunk=%s nodes=%d rels=%d",
				docID,
				chunk.ChunkID,
				len(result.Nodes),
				len(result.Relationships),
			)
		}

		if err := w.writeChunk(ctx, tx, docID, chunk); err != nil {
			return nil, err
		}

		// 2. Batch nodes (grouped by label)
		groupedNodes := groupNodesByLabel(result.Nodes)

		for label, nodes := range groupedNodes {
			if w.Logger != nil {
				w.Logger.Printf("Batch Nodes label=%s size=%d", label, len(nodes))
			}
			if err := w.batchMergeNodesByLabel(ctx, tx, label, nodes); err != nil {
				if w.Logger != nil {
					w.Logger.Printf("Batch Nodes label=%s size=%d", label, len(nodes))
				}
				return nil, err
			}
		}
		// 3. Relationships (grouped by type + labels)
		groupedRels := groupRelationshipsAdvanced(result.Relationships)

		for key, rels := range groupedRels {
			if w.Logger != nil {
				w.Logger.Printf("Batch Rels type=%s size=%d", key.Type, len(rels))
			}
			if err := w.batchMergeRelationshipsWithLabels(ctx, tx, key, rels); err != nil {
				if w.Logger != nil {
					w.Logger.Printf("Batch Rels type=%s size=%d", key.Type, len(rels))
				}
				return nil, err
			}
		}

		if w.Logger != nil {
			w.Logger.Printf("END ingestion doc=%s chunk=%s duration=%s",
				docID,
				chunk.ChunkID,
				time.Since(start),
			)
		}
		return nil, nil
	})

	return err
}

// To Write the user data into Neo4j(actual data)
func (w *Writer) writeChunk(ctx context.Context, tx neo4j.ManagedTransaction, docID string, chunk extractor.Chunk) error {

	query := `
MERGE (d:Document {uid:$doc_uid})
SET d.id = $doc_id

MERGE (c:Chunk {uid : $chunk_uid})
SET c.text =  $text,
	c.index = $idx,
	c.parent_uid = $parent_uid,
	c.doc_uid = $doc_uid,
	c.token_count = $tokens,
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
MATCH (prev:Chunk {uid : $prev_uid})
MATCH (curr:Chunk {uid :$curr_uid})
MERGE (prev)-[:NEXT_CHUNK]->(curr)
`

		_, err := tx.Run(ctx, nextQuery, map[string]any{
			"prev_uid": prevChunkId,
			"curr_uid": chunk.ChunkID,
		})

		if err != nil {
			w.Logger.Printf("ERROR writeChunk doc=%s chunk=%s err=%v",
				docID, chunk.ChunkID, err)
			return err
		}
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
    x.source_id = n.source_id,
    x.confidence = n.confidence,
    x.t_valid = n.t_valid,
    x.t_invalid = n.t_invalid,
    x.t_ingest = n.t_ingest
REMOVE x._lock
`, label)

	_, err := tx.Run(ctx, query, map[string]any{
		"nodes": nodesToMap(nodes),
	})
	return err
}

// it is much faster than batchMergeRelationships Method
func (w *Writer) batchMergeRelationshipsWithLabels(ctx context.Context, tx neo4j.ManagedTransaction, key relGroupKey, rels []extractor.Relationship) error {

	query := fmt.Sprintf(`
UNWIND $relationships AS r
MATCH (a:%s {uid: r.source_uid})
MATCH (b:%s {uid: r.target_uid})
MERGE (a)-[rel:%s {uid: r.uid}]->(b)
SET rel._lock = true
WITH rel, r
SET rel += r.properties,
    rel.source_id = r.source_id,
    rel.confidence = r.confidence,
    rel.t_valid = r.t_valid,
    rel.t_invalid = r.t_invalid,
    rel.t_ingest = r.t_ingest
REMOVE rel._lock
`,
		key.SourceLabel,
		key.TargetLabel,
		key.Type,
	)

	_, err := tx.Run(ctx, query, map[string]any{
		"relationships": relsToMap(rels),
	})

	return err
}

// nodesToMap flatten the MetaData (neo4j is not going to do it itself)
func nodesToMap(nodes []extractor.Node) []map[string]any {
	out := make([]map[string]any, 0, len(nodes))

	for _, n := range nodes {
		out = append(out, map[string]any{
			"uid":        n.UID,
			"name":       n.Name,
			"properties": n.Properties,

			"source_id":  n.Metadata.SourceID,
			"confidence": n.Metadata.Confidence,
			"t_valid":    n.Metadata.TValid,
			"t_invalid":  n.Metadata.TInvalid,
			"t_ingest":   n.Metadata.TIngest,
		})
	}

	return out
}

type relGroupKey struct {
	Type        string
	SourceLabel string
	TargetLabel string
}

// group the relations based on the Type + sourceLabel + TargetLabel
// as the Neo4j is not suitable fro dynamic naming(i donno the name but its like $(r.source_label) is not allowed )
func groupRelationshipsAdvanced(rels []extractor.Relationship) map[relGroupKey][]extractor.Relationship {

	out := make(map[relGroupKey][]extractor.Relationship)

	for _, r := range rels {
		key := relGroupKey{
			Type:        r.Type,
			SourceLabel: r.SourceLabel,
			TargetLabel: r.TargetLabel,
		}
		out[key] = append(out[key], r)
	}

	return out
}

func relsToMap(rels []extractor.Relationship) []map[string]any {
	out := make([]map[string]any, 0, len(rels))

	for _, r := range rels {
		out = append(out, map[string]any{
			"uid": r.UID,

			"source_uid": r.SourceUID,
			"target_uid": r.TargetUID,

			"properties": r.Properties,

			"source_id":  r.Metadata.SourceID,
			"confidence": r.Metadata.Confidence,
			"t_valid":    r.Metadata.TValid,
			"t_invalid":  r.Metadata.TInvalid,
			"t_ingest":   r.Metadata.TIngest,
		})
	}

	return out
}

//====================NO USE FUNCTIONS =======================

func (w *Writer) WriteExtraction(ctx context.Context, docID string, chunk extractor.Chunk, result *extractor.GraphResult) error {

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

// much faster than mergeRelationship slower than batchMergeRelationshipsWithLabels
func (w *Writer) batchMergeRelationships(ctx context.Context, tx neo4j.ManagedTransaction, relType string, rels []extractor.Relationship) error {

	query := fmt.Sprintf(`
UNWIND $rels AS r
MATCH (a:$(r.source_label) {uid: r.source_uid})
MATCH (b:$(r.target_label) {uid: r.target_uid})
MERGE (a)-[rel:%s {uid: r.uid}]->(b)
SET rel._lock = true // Manual write lock to prevent Lost Updates
WITH rel, r
SET rel += r.properties,
    rel.source_id = r.source_id,
    rel.confidence = r.confidence,
    rel.t_valid = r.t_valid,
    rel.t_ingest = datetime()
REMOVE rel._lock`, relType)

	_, err := tx.Run(ctx, query, map[string]any{
		"rels": relsToMap(rels),
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
