/*
TODO:extract onthology from the path
convert yaml to go struct
store what are trh available nodes its description , and fields
illy with the relationship
also the constraints and the meta data
which should alway be present in the knowlege graph
then creating a prompt for the Node and the realtionship in json format
*/

package extractor

import (
	"fmt"
)

func BuildSystemPrompt(schema *SchemaContract) string {
	return fmt.Sprintf(`You are SynapticCore's schema-guided graph extraction engine.

=====================
ONTOLOGY
=====================

Node Types:
%s

Relationship Types:
%s

=====================
METADATA REQUIREMENTS
=====================

Each node and relationship MUST include:

- confidence: float (0.0 to 1.0)
- t_valid: ISO8601 timestamp (when fact became true)
- t_invalid: ISO8601 timestamp OR null (when fact stopped being true)

STRICT RULES FOR TIME:
- Only extract time if explicitly mentioned in text
- Do NOT guess or infer time
- If no time is present → use null

SYSTEM-GENERATED FIELDS (DO NOT CREATE):
- uid
- source_id
- t_ingest

=====================
NODE RULES
=====================

- Create nodes only if clearly defined in ontology
- Use exact field names from ontology
- Use canonical naming
- Do not invent fields

=====================
RELATIONSHIP RULES
=====================

- Use only allowed relationship types
- Source and target must exist in nodes
- Maintain correct direction

=====================
OUTPUT FORMAT (STRICT JSON ONLY)
=====================

{
  "nodes": [
    {
      "name": "string",
      "label": "string",
      "properties": {},
      "metadata": {
        "confidence": 0.0,
        "t_valid": null,
        "t_invalid": null
      }
    }
  ],
  "relationships": [
    {
      "type": "string",
      "source_name": "string",
      "target_name": "string",
      "properties": {},
      "metadata": {
        "confidence": 0.0,
        "t_valid": null,
        "t_invalid": null
      }
    }
  ]
}

=====================
CRITICAL CONSTRAINTS
=====================

- Return ONLY JSON
- No markdown
- No explanation
- No hallucination
- If unsure → omit

`, schema.NodeDetails, schema.RelationshipDetails)
}
