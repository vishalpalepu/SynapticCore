package extractor

import "time"

// 1. LLM OUTPUT MODELS

type LLMMetadata struct {
	Confidence float64 `json:"confidence"`
	TValid     *string `json:"t_valid,omitempty"`
	TInvalid   *string `json:"t_invalid,omitempty"`
}

// Node from LLM no UID
type LLMNode struct {
	Name       string                 `json:"name"`
	Label      string                 `json:"label"`
	Properties map[string]interface{} `json:"properties,omitempty"`
	Metadata   LLMMetadata            `json:"metadata"`
}

// Relationship from LLM no UID(i will generate it) (uses names to connect to node not UID)
type LLMRelationship struct {
	Type        string                 `json:"type"`
	SourceName  string                 `json:"source_name"`
	SourceLabel string                 `json:"source_label"`
	TargetName  string                 `json:"target_name"`
	TargetLabel string                 `json:"target_label"`
	Properties  map[string]interface{} `json:"properties,omitempty"`
	Metadata    LLMMetadata            `json:"metadata"`
}

type LLMExtractionResult struct {
	Nodes         []LLMNode         `json:"nodes"`
	Relationships []LLMRelationship `json:"relationships"`
}

//2. INTERNAL NODES (AFTER TRANSFORMATION)
//Full MetaData (system + LLM)
type Metadata struct {
	UID        string     `json:"uid"`
	SourceID   string     `json:"source_id"`
	Confidence float64    `json:"confidence"`
	TValid     *time.Time `json:"t_valid"`
	TInvalid   *time.Time `json:"t_invalid"`
	TIngest    time.Time  `json:"t_ingest"`
}

// Node (ready for Neo4j)
type Node struct {
	UID        string                 `json:"uid"` //uid is mainly used to avoid duplication
	Name       string                 `json:"name"`
	Label      string                 `json:"label"`
	Properties map[string]interface{} `json:"properties"`
	Metadata   Metadata               `json:"metadata"`
}

//Final Relationship (UID based linking)
type Relationship struct {
	Type        string                 `json:"type"`
	SourceName  string                 `json:"source_name"`
	SourceUID   string                 `json:"source_uid"`
	SourceLabel string                 `json:"source_label"`
	TargetName  string                 `json:"target_name"`
	TargetUID   string                 `json:"target_uid"`
	TargetLabel string                 `json:"target_label"`
	UID         string                 `json:"uid"`
	Properties  map[string]interface{} `json:"properties"`
	Metadata    Metadata               `json:"metadata"`
}

// Final Graph Result
type GraphResult struct {
	Nodes         []Node
	Relationships []Relationship
}

//3. CHUNK MODEL (INPUT PIPELINE)
type Chunk struct {
	DocID      string
	ChunkID    string
	ParentID   string
	Index      int
	Text       string
	TokenCount int
}

type DocumentPackage struct {
	DocID         string         `json:"doc_id"`
	Chunks        []Chunk        `json:"chunks"`
	Nodes         []Node         `json:"nodes"`
	Relationships []Relationship `json:"relationships"`
	TIngest       time.Time      `json:"t_ingest"`
}
