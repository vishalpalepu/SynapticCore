# SynapticCore Development Guide

This document describes how to work on the current SynapticCore implementation without treating unfinished infrastructure as completed functionality.

## 1. Development priorities

The current priority order should be:

1. Stabilize Kafka broker startup.
2. Make topic provisioning deterministic.
3. Make the Kafka integration test pass.
4. Re-run end-to-end extraction → Kafka → Neo4j tests.
5. Add failure/retry tests.
6. Benchmark the optimized writer.
7. Only then expand the reasoning/API layer documentation and testing.

Do not add new infrastructure layers while the ingestion boundary is still failing.

## 2. Local prerequisites

Install:

- Docker Desktop / Docker Engine
- Go
- Python
- Git
- a configured LLM endpoint

Use the repository's `.env.example` as the source of truth for environment variable names.

Never commit:

```text
.env
API keys
database passwords
private certificates
local database volumes
```

## 3. Suggested startup sequence

Start infrastructure first:

```bash
docker compose up -d
```

Then inspect service state:

```bash
docker compose ps
```

Check logs for Kafka before running the integration test:

```bash
docker compose logs kafka
```

If the repository uses a separate topic initialization service:

```bash
docker compose logs kafka-init
```

The initialization service should terminate after successful topic creation. It should not remain in an endless readiness loop.

## 4. Required Kafka topics

The documented architecture uses:

```text
raw-documents
extracted-triples
```

The local development configuration has used:

```text
3 partitions
replication factor 1
```

That replication setting is suitable for a local single-broker environment, not a production durability claim.

## 5. Kafka verification checklist

Before running the test suite, verify the broker:

```bash
docker compose ps
docker compose logs kafka
```

Then verify topics using the Kafka container tooling available in the image.

The expected topics are:

```text
raw-documents
extracted-triples
```

If the integration test reports:

```text
Unknown Topic Or Partition
```

check the following in order:

1. Kafka broker process is actually ready.
2. The bootstrap address used by the test matches the network context where the test runs.
3. The topic exists on the broker.
4. The topic name matches exactly.
5. The topic initializer is not failing before topic creation.
6. The topic initializer is not repeatedly reconnecting to an unhealthy broker.
7. The broker's advertised listeners are reachable from the test process.
8. The test is not starting before the broker has finished initialization.

## 6. Current Kafka blocker

The latest development run recorded:

```text
Failed to write seed document to Kafka:
[3] Unknown Topic Or Partition
```

The documented root cause was that the required topics had not been initialized.

An initializer service was added:

```yaml
kafka-init:
  ...
```

but it waits in a loop until port `9092` is reachable. The latest notes indicate that this waiting behavior may itself be contributing to excessive connection attempts while Kafka is not healthy.

### Required fix

Replace the current "poll forever" behavior with a bounded readiness strategy:

```text
start broker
    ↓
wait for actual broker readiness
    ↓
create topics exactly once
    ↓
exit init container
    ↓
start / test application
```

The implementation should distinguish:

- TCP port open,
- Kafka broker ready to serve metadata,
- topic successfully created.

A port being open is not by itself proof that the Kafka broker is ready for application traffic.

## 7. Kafka consumer contract

The extractor and graph workers should preserve this sequence:

```text
FetchMessage(ctx)
      ↓
process
      ↓
successful downstream persistence
      ↓
CommitMessages(...)
```

The reason is delivery semantics. Kafka describes this style as at-least-once: a message can be delivered again if a consumer crashes after processing but before saving its position.

Reference:
https://kafka.apache.org/41/design/design/

Duplicate processing must therefore remain safe.

## 8. Worker-pool rules

The worker pool is intentionally bounded.

### Extractor worker

The extraction stage is dominated by CPU/network/LLM latency and can use multiple workers.

However, the documented implementation keeps chunk processing for a single document sequential where ordering and chunk continuity are important.

### Graph worker

Neo4j writes should be more conservative.

The graph worker should:

- receive complete `DocumentPackage` objects,
- deserialize them,
- call the graph writer,
- keep each document write atomic,
- avoid one Neo4j transaction per chunk unless explicitly required.

## 9. DocumentPackage rules

A `DocumentPackage` is the unit of graph persistence.

It should contain enough information to reconstruct the entire document graph update:

```text
DocumentPackage
├── document
├── chunks
├── extracted nodes
├── extracted relationships
└── NEXT_CHUNK links
```

The graph writer should persist the package in one managed transaction where practical.

This prevents a document from being left with only a prefix of its chunks when later extraction fails.

## 10. Neo4j schema initialization

Schema initialization is automated from Go rather than requiring the user to manually run the schema script every time.

The documented constraints include:

```cypher
CREATE CONSTRAINT entity_uid IF NOT EXISTS
FOR (n:Entity) REQUIRE n.uid IS UNIQUE;

CREATE CONSTRAINT event_uid IF NOT EXISTS
FOR (n:Event) REQUIRE n.uid IS UNIQUE;

CREATE CONSTRAINT concept_uid IF NOT EXISTS
FOR (n:Concept) REQUIRE n.uid IS UNIQUE;

CREATE CONSTRAINT instruction_uid IF NOT EXISTS
FOR (n:Instruction) REQUIRE n.uid IS UNIQUE;

CREATE CONSTRAINT document_uid IF NOT EXISTS
FOR (n:Document) REQUIRE n.uid IS UNIQUE;

CREATE CONSTRAINT chunk_uid IF NOT EXISTS
FOR (n:Chunk) REQUIRE n.uid IS UNIQUE;
```

Additional lookup indexes are used where appropriate.

Neo4j documents that uniqueness constraints are backed by indexes and prevent duplicate values:
https://neo4j.com/docs/cypher-manual/current/schema/constraints/create-constraints/

## 11. Graph-writing performance rules

Avoid this pattern:

```text
for every node:
    network round-trip
    transaction work
```

Prefer:

```text
collect nodes
collect relationships
    ↓
one parameterized batch
    ↓
UNWIND
    ↓
MERGE / SET
```

The development work specifically changed the writer to reduce round-trips.

The correct GitHub claim is:

> "Batch graph persistence reduces database round-trips and transaction overhead."

Avoid unsupported absolute claims such as:

> "Always 100x faster."

The recorded development notes contain measurements from specific local runs, but those are not a representative benchmark suite.

## 12. UID and normalization testing

The UID pipeline depends on stable normalization.

Test cases should include:

```text
"Google"
" google"
"google  "
"GOOGLE"
"Google   Cloud"
"Google Cloud"
```

The tests should make the intended distinction between:

- normalization used for identity,
- canonical name used for display/preservation of case.

A UID regression can silently create duplicate graph entities, so this deserves unit coverage.

## 13. Chunking tests

Chunking should be tested for:

- empty input,
- very short input,
- input exactly at the target size,
- input slightly above the target,
- large input,
- overlap continuity,
- no missing segments,
- deterministic output.

The development log records an approximate target range and overlap, but the final values should come from configuration rather than hard-coded documentation claims.

## 14. Token-bucket tests

The documented test used:

```text
capacity = 10
refill = 2 tokens/sec
```

Expected behavior:

```text
requests 1..10  → immediate
request 11      → waits for refill
subsequent load → approximately follows refill rate
```

Also test:

- cancellation while waiting,
- concurrent callers,
- zero/invalid configuration,
- no busy-waiting.

## 15. Graceful shutdown tests

The Go service must respond correctly to:

```text
Ctrl+C
SIGTERM
```

Test:

1. Start the service.
2. Make the worker block on Kafka or an in-flight operation.
3. Send termination.
4. Confirm the context is canceled.
5. Confirm the fetch loop exits.
6. Confirm the task channel closes.
7. Confirm workers finish.
8. Confirm the process exits without a goroutine leak.

A previous implementation used `continue` after `FetchMessage` returned a canceled-context error. That produced an infinite high-CPU loop.

The corrected pattern checks `ctx.Err()` and terminates the fetch loop.

## 16. LLM extraction tests

Extraction tests should separate model behavior from infrastructure behavior.

At minimum, test:

### Valid response

```json
{
  "nodes": [],
  "relationships": []
}
```

### Invalid JSON

The transformer should reject malformed output.

### Missing source/target labels

The output should fail validation or be rejected rather than creating an ambiguous relationship.

### LLM-generated Document/Chunk nodes

These should not be accepted because those infrastructure nodes are created deterministically by the system.

### Oversized / truncated response

The transformer should fail safely rather than partially decoding corrupted graph data.

## 17. End-to-end test

The highest-value integration test is:

```text
seed document
   ↓
Kafka raw-documents
   ↓
extractor worker
   ↓
LLM
   ↓
DocumentPackage
   ↓
Kafka extracted-triples
   ↓
graph worker
   ↓
Neo4j transaction
   ↓
offset commit
```

Verification should include:

- document exists,
- expected chunk count exists,
- every chunk belongs to the correct document,
- expected extracted nodes exist,
- expected relationships exist,
- `NEXT_CHUNK` chain is complete,
- duplicate delivery does not create duplicate entities,
- Kafka offset is committed after graph success.

## 18. Failure-injection tests

Once the basic Kafka test passes, intentionally fail each stage:

### Kafka failure

```text
Kafka unavailable
→ application should stop/retry according to policy
```

### LLM failure

```text
LLM unavailable
→ message should not be acknowledged prematurely
```

### Neo4j failure

```text
graph write fails
→ transaction rolls back
→ offset remains uncommitted
→ message is eligible for redelivery
```

### Process crash after Neo4j commit but before offset commit

Expected:

```text
message redelivered
→ idempotent Neo4j write
→ no duplicate graph entities
```

This is the key test for the intended at-least-once + idempotent-write architecture.

## 19. Observability

Current logging has been added to the pipeline.

The next useful metrics are:

- Kafka messages received
- extraction success/failure
- LLM request latency
- LLM error rate
- token-bucket wait duration
- graph transaction latency
- graph transaction failures
- documents committed
- offsets committed
- redelivered documents
- worker utilization

Do not claim Prometheus/Grafana support unless the corresponding implementation exists in the repository.

## 20. Troubleshooting checklist

### Neo4j connection errors

Check:

```text
Is Docker running?
Is the Neo4j container healthy?
Is authentication configured?
Is the correct URI being passed?
Is the driver using the correct URI scheme?
```

The development log previously encountered an invalid URI scheme caused by configuration loading.

### Ontology loading errors

Check:

```text
absolute path
filename spelling
working directory
relative path assumptions
```

A previous test failure was caused by a filename/path mismatch.

### LLM invalid JSON

Check:

```text
model response length
chunk size
prompt constraints
response truncation
```

The development work found that larger context sent to smaller/free-tier models could cause incomplete JSON.

### Duplicate graph entities

Check:

```text
UID normalization
Neo4j uniqueness constraints
MERGE predicates
transaction boundaries
```

## 21. What is safe to claim on GitHub right now

### Safe

- "Schema-guided knowledge extraction"
- "Polyglot microservice architecture"
- "Go-based ingestion engine"
- "Kafka-based asynchronous pipeline"
- "Neo4j graph persistence"
- "Deterministic UID generation"
- "Batch graph writes with UNWIND"
- "Token-bucket rate limiting"
- "Document-level transactional persistence"
- "At-least-once Kafka processing design"
- "Graceful shutdown handling"

### Do not claim yet

- "Production-ready"
- "Exactly-once processing"
- "Guaranteed O(1) graph operations"
- "Zero data loss"
- "Guaranteed 7 ms latency"
- "Fully production-ready MCP reasoning platform"
- "Kafka integration fully working"

Those require repeatable evidence that is not present in the current work log.

## 22. Recommended next implementation sequence

```text
1. Fix Kafka broker readiness
        ↓
2. Make topic provisioning deterministic
        ↓
3. Pass Kafka integration test
        ↓
4. Add duplicate-delivery test
        ↓
5. Add failure-injection tests
        ↓
6. Benchmark DocumentPackage vs row-by-row writes
        ↓
7. Add metrics
        ↓
8. Verify Python reasoning API
        ↓
9. Verify MCP tools
        ↓
10. Verify Redis semantic caching
```

The primary objective should remain a correct, reproducible ingestion pipeline before adding more architectural surface area.

## 23. Documentation discipline

Every future change should update documentation in the same commit when it changes:

- architecture,
- message semantics,
- schema/ontology,
- transaction boundaries,
- configuration,
- failure handling,
- public API behavior.

The documentation should always distinguish:

```text
Implemented
Verified
Planned
Known broken
```

That distinction is especially important for a project intended to demonstrate systems engineering ability.
