package extractor

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type NodeDef struct {
	Description string            `yaml:"description"`
	Fields      map[string]string `yaml:"fields"`
}

type RawOnthology struct {
	Version        string             `yaml:"version"`
	MetadataSchema map[string]string  `yaml:"metadata_schema"`
	Nodes          map[string]NodeDef `yaml:"nodes"`
	Relationships  map[string]string  `yaml:"relationships"`
}

type SchemaContract struct {
	NodeDetails         string
	RelationshipDetails string
	MetadataDetails     string
}

func LoadOnthology(path string) (*SchemaContract, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var raw RawOnthology
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	var nodes strings.Builder
	var rels strings.Builder
	var meta strings.Builder

	// Nodes + Fields
	for name, def := range raw.Nodes {
		nodes.WriteString(fmt.Sprintf("- %s: %s\n", name, def.Description))

		for field, desc := range def.Fields {
			nodes.WriteString(fmt.Sprintf("  • %s: %s\n", field, desc))
		}
	}

	// Relationships
	for name, desc := range raw.Relationships {
		rels.WriteString((fmt.Sprintf("- %s: %s\n", name, desc)))
	}

	// MetaData
	for name, desc := range raw.MetadataSchema {
		meta.WriteString(fmt.Sprintf("- %s: %s\n", name, desc))
	}

	return &SchemaContract{
		NodeDetails:         nodes.String(),
		RelationshipDetails: rels.String(),
		MetadataDetails:     meta.String(),
	}, nil
}
