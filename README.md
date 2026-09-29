# SynapticCore

**Schema-guided knowledge ingestion and graph memory infrastructure**

SynapticCore is a polyglot, event-driven system for converting unstructured text into a structured knowledge graph. The ingestion path is designed around a canonical ontology, deterministic identifiers, chunk-level LLM extraction, Kafka-based buffering, and transactional Neo4j writes.

The current implementation focus is the **high-throughput ingestion path**:

```text
Raw document
    │
    ▼
Kafka: raw-documents
    │
    ▼
Extractor workers
    │
    ├── Chunking
    ├── Token-bucket rate limiting
    ├── Schema-guided LLM extraction
    ├── Validation / normalization
    └── Deterministic UID enrichment
    │
    ▼
Kafka: extracted-triples
    │
    ▼
Graph workers
    │
    └── DocumentPackage
          ├── Document / Chunk nodes
          ├── Extracted nodes
          ├── Relationships
          └── NEXT_CHUNK lexical links
    │
    ▼
Neo4j
```

> **Current status:** the Neo4j ingestion path has been tested and the Kafka path has been exercised, but the Kafka integration test is currently blocked by broker/topic initialization. The repository should therefore be presented as an actively developed system rather than a fully production-ready platform.

## Why SynapticCore exists

LLM-generated knowledge graphs tend to become difficult to maintain when the model is allowed to invent arbitrary node labels, relationships, identifiers, and metadata.

SynapticCore takes a **contract-first** approach:

1. Define a canonical ontology.
2. Give the model only the allowed node and relationship vocabulary.
3. Validate and normalize the model output.
4. Generate system-owned identifiers and ingestion metadata outside the model.
5. Persist a complete document as one logical graph package.
6. Use Kafka to decouple ingestion from downstream extraction and graph persistence.

This separates model-generated facts from system-generated identity and lifecycle metadata.

## Current implementation

### Implemented

- Cross-domain ontology with abstract node types:
  - `Entity`
  - `Event`
  - `Concept`
  - `Instruction`
- Generalized relationship types such as:
  - `PART_OF`
  - `ASSOCIATED_WITH`
  - `DEPEND_ON`
  - `TRIGGERED_BY`
  - `MENTIONED_IN`
  - `PERFORMED_BY`
- Bi-temporal metadata model using fields such as:
  - `uid`
  - `source_id`
  - `confidence`
  - `t_valid`
  - `t_invalid`
  - `t_ingest`
- Pydantic models mirroring the ontology contract.
- Go-based extraction pipeline.
- Deterministic UID generation using normalized identifiers and SHA-256.
- Input chunking with configurable overlap.
- Token-bucket rate limiting around LLM calls.
- Transformation pipeline:
  - prompt construction
  - LLM extraction
  - JSON decoding
  - normalization
  - ontology validation
  - system metadata enrichment
- Neo4j transactional writes.
- Batch graph persistence with Cypher `UNWIND`.
- Deterministic `Document` and `Chunk` handling.
- `NEXT_CHUNK` lexical links created by the ingestion system rather than the LLM.
- Neo4j uniqueness constraints and supporting indexes.
- Automatic schema initialization from Go code.
- Pessimistic locking for hot-node updates in the high-concurrency ingestion path.
- Kafka producer/consumer architecture.
- Separate `raw-documents` and `extracted-triples` stages.
- Worker-pool execution.
- Manual Kafka offset control design for at-least-once processing.
- Context propagation and graceful shutdown.
- Logging.
- Manual Neo4j ingestion testing.
- Document-level batch persistence through `DocumentPackage`.

### Architecture already defined but not treated as fully verified here

The repository layout also defines a Python reasoning service containing FastAPI, GraphRAG/multi-hop reasoning, MCP integration, and Redis semantic caching. Those components are part of the intended architecture, but the current development log is primarily evidence for the ingestion subsystem. Do not describe the reasoning API as production-complete unless its implementation and tests are separately verified.

## Core design decisions

### 1. Contract-first extraction

The LLM is not the owner of the graph schema. The ontology defines the permitted entities and relationships, and the extraction prompt is generated from that contract.

This reduces label drift and invalid relationship types and makes downstream graph data predictable.

### 2. System-owned identity

The LLM does not generate authoritative `uid`, `source_id`, or `t_ingest` values.

The system creates those fields after model extraction. This keeps identity and provenance under deterministic application control.

### 3. Document-level graph transactions

A document may generate many chunks, nodes, and relationships. These are collected into a `DocumentPackage` and written as a single logical transaction.

The intent is to avoid partially persisted documents in which only some chunks or relationships have been committed.

Neo4j provides ACID transactional behavior, so a failed transaction can roll back the entire graph update. See the official Neo4j transaction documentation:
https://neo4j.com/docs/operations-manual/current/database-internals/

### 4. Batch persistence

The optimized writer uses Cypher `UNWIND` to send sets of graph data in batches instead of issuing a separate database round-trip for every individual item.

This reduces client/database round-trips and transaction overhead.

### 5. Idempotent node creation

Nodes are identified through deterministic UIDs and Neo4j uniqueness constraints. Neo4j documents that property uniqueness constraints are backed by indexes and are intended to enforce uniqueness efficiently:
https://neo4j.com/docs/cypher-manual/current/schema/constraints/create-constraints/

The project should avoid claiming a universal `O(1)` wall-clock guarantee. The accurate statement is that indexed lookup avoids an unindexed scan and scales much better as the graph grows.

### 6. Kafka as the asynchronous boundary

Kafka separates raw document arrival from slower, rate-limited extraction and graph persistence.

The intended stages are:

```text
raw-documents
    │
    ▼
Extractor Worker
    │
    ▼
extracted-triples
    │
    ▼
Graph Worker
```

The implementation uses manual offset control so the downstream graph write succeeds before the Kafka offset is acknowledged. This is an **at-least-once** processing design; duplicates are possible and are handled through idempotent graph writes.

Apache Kafka documents at-least-once processing as the case where a consumer processes a message and saves its position afterward, meaning a crash between those steps can cause redelivery:
https://kafka.apache.org/41/design/design/

### 7. Graceful shutdown

The Go services propagate a cancellable `context.Context` from the process entry point through Kafka and database operations.

The worker loop must terminate when the context is canceled rather than retrying indefinitely. Worker goroutines are tracked and the message channel is closed before waiting for workers to finish.

## Repository structure

```text
synaptic-core/
├── services/
│   ├── ingestion-engine-go/
│   │   ├── cmd/
│   │   ├── internal/
│   │   │   ├── kafka/
│   │   │   ├── graph/
│   │   │   ├── limiter/
│   │   │   └── extractor/
│   │   ├── go.mod
│   │   └── Dockerfile
│   │
│   └── reasoning-api-python/
│       ├── app/
│       │   ├── api/
│       │   ├── core/
│       │   ├── mcp/
│       │   ├── cache/
│       │   └── tools/
│       ├── requirements.txt
│       └── Dockerfile
│
├── infrastructure/
│   ├── docker-compose.yml
│   ├── neo4j/
│   ├── kafka/
│   └── redis/
│
├── shared/
│   └── docs/
│       ├── architecture.md
│       └── ontology-v1.yaml
│
├── .gitignore
└── .env.example
```

## Local stack

The intended local environment contains:

- Neo4j for graph storage.
- Kafka for asynchronous ingestion.
- Redis for the planned semantic-cache layer.
- Go ingestion service.
- Python reasoning service.

Docker Compose is used as the local orchestration layer.

### Prerequisites

- Docker Desktop / Docker Engine
- Go toolchain compatible with the repository
- Python environment for the reasoning service
- Access to the configured LLM endpoint
- Git

Use `.env.example` as the source of truth for environment configuration. Do not commit real credentials.

## Getting started

Because Kafka topic provisioning is the current known blocker, the repository should not advertise a guaranteed one-command startup until broker initialization and topic creation are stable.

A safe local workflow is:

```bash
git clone <repository-url>
cd synaptic-core
```

Then:

1. Configure local environment values from `.env.example`.
2. Start the infrastructure with Docker Compose.
3. Verify Neo4j is reachable.
4. Verify Kafka is healthy.
5. Verify the required topics exist:
   - `raw-documents`
   - `extracted-triples`
6. Run the ingestion service.
7. Run the relevant tests.

For detailed development and troubleshooting procedures, see [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md).

## Data model

The canonical ontology is maintained in:

```text
shared/docs/ontology-v1.yaml
```

The ontology is mirrored into typed application models rather than allowing each LLM call to invent its own schema.

The graph also uses infrastructure-owned labels for documents and chunks. These are deliberately not extracted from free-form text by the LLM.

## Testing status

| Area | Status |
|---|---|
| UID generation | Implemented |
| Chunking | Implemented |
| Token-bucket limiter | Tested |
| Neo4j constraints/indexes | Implemented |
| Neo4j manual ingestion | Tested |
| DocumentPackage batch write | Tested |
| Graceful shutdown path | Implemented |
| Kafka consumer/worker code | Implemented |
| Kafka end-to-end integration | **Blocked** |
| Kafka topic initialization in Compose | **Blocked / under investigation** |
| Python reasoning layer | Not verified by this development log |
| MCP layer | Not verified by this development log |
| Redis semantic cache | Not verified by this development log |

## Known limitations

### Kafka broker/topic initialization

The latest recorded integration test failed with:

```text
Unknown Topic Or Partition
```

The immediate finding was that the required topics were not initialized before the test attempted to produce data.

A `kafka-init` Compose service was added, but the recorded implementation became stuck waiting for the broker and appears to contribute to connection pressure. This remains the current blocker.

Do not mark Kafka integration as complete until:

- the broker reaches a healthy state,
- topic creation succeeds deterministically,
- the integration test can produce and consume messages,
- offsets are committed only after successful downstream persistence,
- restart/retry behavior is verified.

### LLM output truncation

Development testing showed that smaller/free-tier models could return incomplete JSON for larger contexts. The pipeline was adjusted to handle truncation, but model capacity and input chunk size remain practical constraints.

### Production readiness

The current project demonstrates architectural and implementation work but should not yet be described as production-ready. Production claims require repeatable benchmarks, failure-injection tests, observability, security review, and deployment validation.

## Documentation map

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — system design and data flow.
- [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md) — local development, testing, operational checks, and known failure modes.
- [`shared/docs/ontology-v1.yaml`](shared/docs/ontology-v1.yaml) — canonical schema contract.
- [`shared/docs/architecture.md`](shared/docs/architecture.md) — keep this aligned with the public architecture documentation if it is retained.

## Project principles

1. **The ontology is the contract.**
2. **The LLM extracts; the system owns identity and provenance.**
3. **Persist complete documents atomically where possible.**
4. **Use Kafka for asynchronous decoupling, not as the graph database.**
5. **Use deterministic identifiers and database constraints for idempotent writes.**
6. **Treat graceful shutdown and retry behavior as part of correctness, not polish.**
7. **Document verified behavior separately from intended architecture.**

## References

- GitHub — About repository README files: https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-readmes
- GitHub — Repository best practices: https://docs.github.com/en/repositories/creating-and-managing-repositories/best-practices-for-repositories
- Apache Kafka — Design and message delivery semantics: https://kafka.apache.org/41/design/design/
- Neo4j — Constraints: https://neo4j.com/docs/cypher-manual/current/schema/constraints/create-constraints/
- Neo4j — Transactional behavior: https://neo4j.com/docs/operations-manual/current/database-internals/
- Model Context Protocol — Server primitives: https://modelcontextprotocol.io/specification/draft/server/index
