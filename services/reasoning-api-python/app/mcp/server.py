import os
import yaml
import tiktoken
import atexit
from datetime import datetime
from typing import Any,Dict,List
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from neo4j import GraphDatabase



load_dotenv()   
mcp = FastMCP("SynapticCore-Reasoning-Engine")


NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
QUERY_TIMEOUT = float(os.getenv("QUERY_TIMEOUT","10.0"))

ONTOLOGY_PATH = os.getenv(
    "ONTOLOGY_PATH",
    "shared/docs/ontology-v1.yaml"
)


driver = GraphDatabase.driver(NEO4J_URI,auth =NEO4J_AUTH)
tokenizer = tiktoken.encoding_for_model("gpt-4o")

atexit.register(driver.close)



ALLOWED_PREFIXES = (
    "MATCH",
    "WITH",
    "OPTIONAL MATCH",
    "CALL"
)

FORBIDDEN_KEYWORDS = (
    "CREATE",
    "MERGE",
    "DELETE",
    "SET",
    "REMOVE",
    "LOAD CSV",
    "FOREACH"
)


def validate_read_query(query: str) -> bool:
    q = query.strip().upper()
    if not q.startswith(ALLOWED_PREFIXES):
        return False
    for word in FORBIDDEN_KEYWORDS:
        if word in q:
            return False
    return True




# Conext Guards 
def sanitize_result(data: Any) ->Any:
    """Recursively prunes noisy embeddings and oversized lists to save tokens."""
    if(isinstance(data,list)):
        if(len(data) > 52):
            return f""
        return [sanitize_result(i) for i in data]
    if(isinstance(data,dict)):
        #we prune the raw vector embedding which are noise to the LLM 
        return {k : sanitize_result(v) for k , v in data.items() if "embedding" not in k.lower()} #it return a dict where each value is sanitized     
    return data

# truncating the string to be below the limit
def truncate_tokens(content: str,limit: int =   2048) ->str:
    tokens = tokenizer.encode(content)
    if(len(tokens) > limit):
        return tokenizer.decode(tokens[:limit]) + "\n..." # will prune the content if above the token limit of 2048
    return content



@mcp.tool
def get_ontology() -> str:
    """
    Returns the canonical ontology used by SynapticCore.
    This allows the LLM to understand the graph schema.
    """
    try:
        with open(ONTOLOGY_PATH, "r") as f:
            ontology = yaml.safe_load(f)
        return yaml.dump(ontology)
    except Exception as e:
        return f"Failed to load ontology: {str(e)}"

# Tool A: Semantic Schema Discovery
@mcp.tool
def get_graph_schema()->str:
    """
    Lightweight schema discovery.
    Much cheaper than apoc.meta.schema().
    """
    
    query = """ 
    CALL {
        CALL db.labels()
        YIELD label
        RETURN collect(label) AS labels
    }
    CALL {
        CALL db.relationshipTypes()
        YIELD relationshipType
        RETURN collect(relationshipType) AS rels
    }
    RETURN labels, rels
    """
    try:
        with driver.session() as session:
            # schema = session.execute_read(lambda tx : tx.run('CALL apoc.meta.schema()').data())
            schema = session.execute_read(lambda tx : tx.run(query,timeout=QUERY_TIMEOUT).data())
            #return in YAML to save tokens
            return yaml.dump(sanitize_result(schema))
    except Exception as e:
        return f"Failed to get schema"

#  Tool B: Disciplined Read-Only Retrieval
@mcp.tool
def read_memory_cypher(query: str)->str:
    """Executes read-only multi-hop traversals against SynapticCore memory."""
    if not validate_read_query(query):
        return "SECURITY ERROR: Only read-only MATCH queries allowed."

    
    try:
        with driver.session() as session:
            # Enforce 10s timeout to prevent Cartesian product hangs [1]
            result = session.execute_read(lambda tx: tx.run(query, timeout=QUERY_TIMEOUT).data())
            return truncate_tokens(yaml.dump(sanitize_result(result)))
    except Exception as e: # Fixed: Correct Python exception syntax
        return f"Query Failed: {str(e)}"


# Tool C: Reasoning Memory Recording
@mcp.tool()
def record_reasoning_trace(objective: str, steps: List[str], outcome: str, agent_id: str) -> str:
    """Writes procedural reasoning traces as Instruction nodes."""

    query = """
    CREATE (i:Instruction {
        uid: apoc.create.uuid(),
        source_id: "agent_reasoning",
        confidence: 1.0,

        t_valid: datetime(),
        t_invalid: null,
        t_ingest: datetime(),

        objective: $objective,
        parameters: {},
        steps: $steps,
        outcome: $outcome,
        performed_by: $agent_id
    })
    RETURN i.uid AS trace_id
    """

    with driver.session() as session:
        res = session.execute_write(
            lambda tx: tx.run(
                query,
                objective=objective,
                steps=steps,
                outcome=outcome,
                agent_id=agent_id
            ).single()
        )

    return f"Trace recorded with ID: {res['trace_id']}"
    
if __name__ == "__main__":
    mcp.run()