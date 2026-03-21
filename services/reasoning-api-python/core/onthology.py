from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Any, Dict, List
from enum import Enum


class Metadata(BaseModel):
    """
    Shared provenance and temporal metadata.
    """
    uid: str = Field(..., description="Deterministic identifier")

    source_id: str = Field(
        ...,
        description="Source document or chunk identifier"
    )

    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Extraction confidence score"
    )

    t_valid: datetime = Field(
        ...,
        description="When the fact became valid"
    )

    t_invalid: Optional[datetime] = Field(
        None,
        description="When the fact ceased being valid"
    )

    t_ingest: datetime = Field(
        ...,
        description="When the record was created in the system"
    )


class GraphNode(Metadata):
    """Base class for all graph nodes."""
    pass


class Entity(GraphNode):

    name: str = Field(
        ...,
        description="Canonical entity name",
        json_schema_extra = {"examples":["Dr. John Doe", "Acme Corporation"]}
    )

    type_label: str = Field(
        ...,
        description="Subtype like Surgeon, Microservice, Project",
        json_schema_extra= {"examples":["Surgeon", "Microservice"]}
    )

    description: Optional[str] = Field(
        None,
        description="LLM generated summary"
    )

    attributes: Dict[str, Any] = Field(
        default_factory=dict,
        description="Domain specific attributes"
    )


class Event(GraphNode):

    event_name: str = Field(
        ...,
        description="Canonical name of the event",
        json_schema_extra = {"examples":["Heart Surgery", "API Request"]}
    )

    status: Optional[str] = Field(
        None,
        description="Scheduled, Completed, Failed"
    )

    start_time: datetime

    end_time: Optional[datetime] = None


class Concept(GraphNode):

    term: str

    definition: str

    domain_context: Optional[str] = None


class Instruction(GraphNode):

    objective: str

    parameters: Dict[str, Any] = Field(default_factory=dict)

    steps: List[str] = Field(default_factory=list)

    outcome: Optional[str] = None

    performed_by: Optional[str] = None


class RelationshipType(str, Enum):

    PART_OF = "PART_OF"
    ASSOCIATED_WITH = "ASSOCIATED_WITH"
    DEPENDS_ON = "DEPENDS_ON"
    TRIGGERED_BY = "TRIGGERED_BY"
    PERFORMED_BY = "PERFORMED_BY"
    MENTIONED_IN = "MENTIONED_IN"


class GraphEdge(Metadata):

    source_uid: str = Field(...)

    target_uid: str = Field(...)


class Relationship(GraphEdge):

    relationship_type: RelationshipType

    properties: Dict[str, Any] = Field(default_factory=dict)