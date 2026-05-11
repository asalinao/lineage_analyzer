from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any


class DatasetRole(StrEnum):
    INPUT = "input"
    OUTPUT = "output"


class EventType(StrEnum):
    START = "START"
    COMPLETE = "COMPLETE"
    FAIL = "FAIL"
    FAILED = "FAILED"
    ABORT = "ABORT"
    ABORTED = "ABORTED"
    OTHER = "OTHER"


class ChangeType(StrEnum):
    ADDED = "ADDED"
    REMOVED = "REMOVED"
    MODIFIED = "MODIFIED"
    LOGIC_CHANGED = "LOGIC_CHANGED"


@dataclass(frozen=True, slots=True)
class AttributeSpec:
    name: str
    data_type: str = "unknown"


@dataclass(frozen=True, slots=True)
class TableSpec:
    namespace: str
    schema: str
    table_name: str
    attributes: tuple[AttributeSpec, ...] = ()

    @property
    def name(self) -> str:
        return f"{self.namespace}.{self.schema}.{self.table_name}"


@dataclass(frozen=True, slots=True)
class TransformationSpec:
    output_table: str
    output_attribute: str
    input_attributes: tuple[tuple[str, str], ...] = ()
    source: str = "openlineage"
    expression: str | None = None
    lineage_type: str | None = None
    lineage_subtype: str | None = None
    lineage_description: str | None = None
    lineage_masking: bool | None = None
    lineage_scope: str = "FIELD"


@dataclass(frozen=True, slots=True)
class JobSpec:
    namespace: str
    name: str
    sql: str | None = None
    dialect: str | None = None


@dataclass(frozen=True, slots=True)
class LineageEvent:
    event_type: EventType
    event_time: datetime
    job: JobSpec
    inputs: tuple[TableSpec, ...] = ()
    outputs: tuple[TableSpec, ...] = ()
    transformations: tuple[TransformationSpec, ...] = ()
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class AttributeChange:
    table_name: str
    attribute_name: str
    change_type: ChangeType
    old_type: str | None = None
    new_type: str | None = None


@dataclass(frozen=True, slots=True)
class TransformationChange:
    job_name: str
    output_table: str | None
    output_attribute: str | None
    change_type: ChangeType
    input_table: str | None = None
    input_attribute: str | None = None
    expression: str | None = None


@dataclass(frozen=True, slots=True)
class ImpactPath:
    source_table: str
    source_attribute: str
    affected_table: str
    affected_attribute: str
    depth: int


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
