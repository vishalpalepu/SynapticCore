package extractor

// ingestion orchestration
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type LLMCLient interface {
	Extract(ctx context.Context, systemPrompt, userPrompt string) (string, error)
}

type TokenLimiter interface {
	Acquire(ctx context.Context) error // token from token bucket before LLM call will wait for a token to be provided
}

type Transformer struct {
	Schema  *SchemaContract
	Client  LLMCLient
	Limiter TokenLimiter
}

func (t *Transformer) ExtractChunk(ctx context.Context, chunk Chunk) (*GraphResult, error) {
	if t.Limiter != nil {
		if err := t.Limiter.Acquire(ctx); err != nil {
			return nil, err
		}
	}

	systemPrompt := BuildSystemPrompt(t.Schema)
	userPrompt := fmt.Sprintf(
		"Extract graph data from this text.\n\nSource Chunk ID: %s\nDocument ID: %s\n\nText:\n%s",
		chunk.ChunkID,
		chunk.DocID,
		chunk.Text,
	)

	raw, err := t.Client.Extract(ctx, systemPrompt, userPrompt)
	if err != nil {
		return nil, err
	}

	raw = cleanLLMOutput(raw)

	var llmOut LLMExtractionResult
	// In Go, json.Unmarshal is designed to work with byte slices ([]byte)
	// rather than strings because bytes are the most "pure" form of data for transmission and storage.
	if err := json.Unmarshal([]byte(raw), &llmOut); err != nil {
		return nil, fmt.Errorf("invalid JSON from model: %w", err)
	}

	graph := normalizeExtraction(&llmOut, chunk)
	if err := validateExtraction(graph, t.Schema); err != nil {
		return nil, err
	}
	return graph, nil
}

func (t *Transformer) ExtractChunkOptimized(ctx context.Context, chunk Chunk) (*GraphResult, error) {

	start := time.Now()

	// 1. Rate limit
	if t.Limiter != nil {
		if err := t.Limiter.Acquire(ctx); err != nil {
			return nil, err
		}
	}

	systemPrompt := BuildSystemPrompt(t.Schema)
	userPrompt := fmt.Sprintf(
		"Extract graph data.\n\nChunkID: %s\nDocID: %s\n\nText:\n%s",
		chunk.ChunkID,
		chunk.DocID,
		chunk.Text,
	)

	// 2. Retry logic (LLM is unreliable)
	// this the update we retry to call the LLM three times then give up if error occures we wait 500ms then 1000ms then 1500ms each call
	var raw string
	var err error

	for i := 0; i < 3; i++ {
		raw, err = t.Client.Extract(ctx, systemPrompt, userPrompt)
		if err == nil {
			break
		}
		time.Sleep(time.Duration(i+1) * 500 * time.Millisecond)
	}
	if err != nil {
		return nil, fmt.Errorf("llm failed after retries: %w", err)
	}

	raw = cleanLLMOutput(raw)

	// 3. Parse
	var llmOut LLMExtractionResult
	if err := json.Unmarshal([]byte(raw), &llmOut); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	// 4. Normalize + Build Graph + relLabel(added)
	graph := normalizeExtractionOptimized(&llmOut, chunk)

	// 5. Validate
	if err := validateExtraction(graph, t.Schema); err != nil {
		return nil, err
	}

	// 6. Observability (VERY IMPORTANT)
	duration := time.Since(start)
	fmt.Printf("Chunk %s processed in %v (nodes=%d rels=%d)\n",
		chunk.ChunkID,
		duration,
		len(graph.Nodes),
		len(graph.Relationships),
	)

	return graph, nil
}

func cleanLLMOutput(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "```json") // is LLM return the json in .md + json code format
	s = strings.TrimPrefix(s, "```")     // if it in code format
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, "```") // trailing code format
	//removing all of the trailings
	return strings.TrimSpace(s)
}

func normalizeExtraction(out *LLMExtractionResult, chunk Chunk) *GraphResult {
	now := time.Now().UTC()
	nodes := make([]Node, 0, len(out.Nodes))
	nameToUID := make(map[string]string)

	for _, n := range out.Nodes {
		name := CanonicalName(n.Name)
		label := SanitizeLabel(n.Label)

		uid := MakeUID(label, name)

		if n.Properties == nil {
			n.Properties = map[string]any{}
		}

		n.Properties["canonical_name"] = name

		node := Node{
			Name:       name,
			Label:      label,
			UID:        uid,
			Properties: n.Properties,
			Metadata: Metadata{
				UID:        uid,
				SourceID:   chunk.ChunkID,
				TIngest:    now,
				Confidence: clampConfidence(n.Metadata.Confidence),
				TValid:     parseTimePtr(n.Metadata.TValid),
				TInvalid:   parseTimePtr(n.Metadata.TInvalid),
			},
		}
		nodes = append(nodes, node)
		nameToUID[name] = uid
	}

	rels := make([]Relationship, 0, len(out.Relationships))

	for _, r := range out.Relationships {
		srcName := CanonicalName(r.SourceName)
		tarName := CanonicalName(r.TargetName)

		srcUID := nameToUID[srcName]
		tarUID := nameToUID[tarName]

		if srcUID == "" || tarUID == "" {
			continue
		}

		relType := SanitizeLabel(r.Type)
		relBaseUID := srcUID + "|" + relType + "|" + tarUID
		uid := MakeUID("relationship", relBaseUID)

		if r.Properties == nil {
			r.Properties = map[string]any{}
		}

		rels = append(rels, Relationship{
			Type:       relType,
			SourceName: srcName,
			TargetName: tarName,
			SourceUID:  srcUID,
			TargetUID:  tarUID,
			UID:        uid,
			Properties: r.Properties,
			Metadata: Metadata{
				UID:        uid,
				SourceID:   chunk.ChunkID,
				TIngest:    now,
				Confidence: clampConfidence(r.Metadata.Confidence),
				TValid:     parseTimePtr(r.Metadata.TValid),
				TInvalid:   parseTimePtr(r.Metadata.TInvalid),
			},
		})
	}

	return &GraphResult{
		Nodes:         nodes,
		Relationships: rels,
	}
}

// For Optimized relationship Query
func normalizeExtractionOptimized(out *LLMExtractionResult, chunk Chunk) *GraphResult {
	now := time.Now().UTC()
	nodes := make([]Node, 0, len(out.Nodes))
	// not using the nameToUID we can create the UID with the label and name itseld instead of the nodes UID

	for _, n := range out.Nodes {
		name := CanonicalName(n.Name)
		label := SanitizeLabel(n.Label)

		uid := MakeUID(label, name)

		if n.Properties == nil {
			n.Properties = map[string]any{}
		}
		n.Properties["canonical_name"] = name

		node := Node{
			Name:       name,
			Label:      label,
			UID:        uid,
			Properties: n.Properties,
			Metadata: Metadata{
				UID:        uid,
				SourceID:   chunk.ChunkID,
				Confidence: clampConfidence(n.Metadata.Confidence),
				TValid:     parseTimePtr(n.Metadata.TValid),
				TInvalid:   parseTimePtr(n.Metadata.TInvalid),
				TIngest:    now,
			},
		}
		nodes = append(nodes, node)
	}

	rels := make([]Relationship, 0, len(out.Relationships))

	for _, r := range out.Relationships {
		srcName := CanonicalName(r.SourceName)
		tarName := CanonicalName(r.TargetName)
		srcLabel := SanitizeLabel(r.SourceLabel)
		tarLabel := SanitizeLabel(r.TargetLabel)

		srcUID := MakeUID(srcLabel, srcName)
		tarUID := MakeUID(tarLabel, tarName)

		if srcUID == "" || tarUID == "" {
			continue
		}

		relType := SanitizeLabel(r.Type)
		relBaseUID := srcUID + "|" + relType + "|" + tarUID
		uid := MakeUID("relationship", relBaseUID)

		if r.Properties == nil {
			r.Properties = map[string]any{}
		}

		rels = append(rels, Relationship{
			Type:        relType,
			SourceName:  srcName,
			TargetName:  tarName,
			SourceUID:   srcUID,
			TargetUID:   tarUID,
			SourceLabel: srcLabel,
			TargetLabel: tarLabel,
			UID:         uid,
			Properties:  r.Properties,
			Metadata: Metadata{
				UID:        uid,
				SourceID:   chunk.ChunkID,
				Confidence: clampConfidence(r.Metadata.Confidence),
				TValid:     parseTimePtr(r.Metadata.TValid),
				TInvalid:   parseTimePtr(r.Metadata.TInvalid),
				TIngest:    now,
			},
		})
	}

	return &GraphResult{
		Nodes:         nodes,
		Relationships: rels,
	}
}

func validateExtraction(graph *GraphResult, schema *SchemaContract) error {

	for _, n := range graph.Nodes {
		if n.Name == "" || n.Label == "" {
			return fmt.Errorf("node identity failure: missing name or label")
		}
		if !schema.AllowedLabels[n.Label] {
			return fmt.Errorf("ontological breach: invalid node label '%s'", n.Label)
		}
	}

	for _, r := range graph.Relationships {
		if r.Type == "" || r.SourceUID == "" || r.TargetUID == "" {
			return fmt.Errorf("triple corruption: missing type or endpoints")
		}

		if !schema.AllowedRels[r.Type] {
			return fmt.Errorf("ontological breach: invalid relationship type '%s'", r.Type)
		}

		if r.SourceLabel == "" || r.TargetLabel == "" {
			return fmt.Errorf("index failure: relationship missing endpoint labels for UID %s", r.UID)
		}
	}

	return nil
}

func parseAllowedItems(block string) map[string]bool {
	mpp := make(map[string]bool)
	lines := strings.Split(block, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "- ") {
			continue
		}

		line = strings.TrimPrefix(line, "- ")
		if idx := strings.Index(line, ":"); idx > 0 {
			key := strings.TrimSpace(line[:idx])
			if key != "" {
				mpp[key] = true
			}
		}
	}

	return mpp
}

func clampConfidence(c float64) float64 {
	if c < 0 || c > 1 {
		return 0.0 // follow the contract strictly

	}
	return c
}

func parseTimePtr(s *string) *time.Time {
	if s == nil || *s == "" {
		return nil
	}

	t, err := time.Parse(time.RFC3339, *s)
	if err != nil {
		return nil // or log error
	}

	return &t
}

//
// HTTP CLIENTS WHICH ACTUALLY TALK TO LLM

type HTTPLLMClient struct {
	BaseURL string
	APIKey  string
	Model   string
}

func (c *HTTPLLMClient) Extract(ctx context.Context, systemPrompt, userPrompt string) (string, error) {
	payload := map[string]any{
		"model": c.Model,
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": userPrompt},
		},
		"temperature": 0.0,
	}

	b, err := json.Marshal(payload)

	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL, bytes.NewReader(b))
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.APIKey)

	client := &http.Client{Timeout: 45 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return "", errors.New(resp.Status)
	}

	var decoded struct {
		Output string `json:"output"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", err
	}

	return decoded.Output, nil
}
