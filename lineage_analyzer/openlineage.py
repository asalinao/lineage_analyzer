from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .models import AttributeSpec, EventType, JobSpec, LineageEvent, TableSpec, TransformationSpec


def parse_openlineage_event(payload: dict[str, Any]) -> LineageEvent:
    run_data = payload.get("run") or {}
    run_id = run_data.get("run_id") or run_data.get("runId")
    if not run_id:
        raise ValueError("OpenLineage run.run_id/runId is required")

    event_type_value = payload.get("eventType")
    try:
        event_type = EventType(str(event_type_value))
    except ValueError:
        event_type = EventType.OTHER

    event_time_value = payload.get("eventTime")
    if event_time_value:
        event_time = datetime.fromisoformat(str(event_time_value).replace("Z", "+00:00"))
    else:
        event_time = datetime.now(timezone.utc)

    return LineageEvent(
        run_id=str(run_id),
        event_type=event_type,
        event_time=event_time,
        job=job_from_openlineage_event(payload.get("job") or {}),
        raw=payload,
    )


def job_from_openlineage_event(job_data: dict[str, Any]) -> JobSpec:
    job_facets = job_data.get("facets") or {}
    sql_facet = job_facets.get("sql") or job_facets.get("sqlQuery") or {}
    return JobSpec(
        namespace=str(job_data.get("namespace") or ""),
        name=str(job_data.get("name") or "unknown_job"),
        sql=sql_facet.get("query") or sql_facet.get("sql"),
        dialect=sql_facet.get("dialect"),
    )


def tables_from_openlianege_event(
    inputs_data: list[dict[str, Any]],
    outputs_data: list[dict[str, Any]],
) -> tuple[tuple[TableSpec, ...], tuple[TableSpec, ...]]:
    tables: list[tuple[TableSpec, ...]] = []
    for datasets in (inputs_data, outputs_data):
        parsed_tables: list[TableSpec] = []
        for dataset in datasets:
            schema = ((dataset.get("facets") or {}).get("schema") or {}).get("fields") or []
            attributes = tuple(
                AttributeSpec(name=str(field.get("name")), data_type=str(field.get("type") or "unknown"))
                for field in schema
                if field.get("name")
            )
            parsed_tables.append(
                TableSpec(
                    namespace=str(dataset.get("name") or "unknown.unknown_table").split(".")[0],
                    schema=(
                        str(dataset.get("name") or "unknown.unknown_table").split(".")[1]
                        if len(str(dataset.get("name") or "unknown.unknown_table").split(".")) > 2
                        else "public"
                    ),
                    table_name=str(dataset.get("name") or "unknown_table").split(".")[-1],
                    attributes=attributes,
                )
            )
        tables.append(tuple(parsed_tables))
    return tables[0], tables[1]


def transformations_from_openlienage_event(outputs_data: list[dict[str, Any]]) -> tuple[TransformationSpec, ...]:
    transformations: list[TransformationSpec] = []
    for output_dataset in outputs_data:
        output_name = str(output_dataset.get("name") or "unknown_table")
        output_table = output_name
        column_lineage_facet = (output_dataset.get("facets") or {}).get("columnLineage") or {}

        for output_attribute, lineage in (column_lineage_facet.get("fields") or {}).items():
            for field in lineage.get("inputFields") or []:
                if not field.get("name") or not field.get("field"):
                    continue
                input_name = str(field.get("name") or "")
                input_table = input_name
                for transformation in field.get("transformations") or []:
                    if not isinstance(transformation, dict):
                        continue
                    lineage_type = transformation.get("type")
                    lineage_subtype = transformation.get("subtype")
                    masking = transformation.get("masking")
                    transformations.append(
                        TransformationSpec(
                            output_table=output_table,
                            output_attribute=str(output_attribute),
                            input_attributes=((input_table, str(field.get("field") or "")),),
                            source="openlineage",
                            lineage_type=None if lineage_type is None else str(lineage_type).upper(),
                            lineage_subtype=None if lineage_subtype is None else str(lineage_subtype).upper(),
                            lineage_description=transformation.get("description"),
                            lineage_masking=masking if isinstance(masking, bool) else None,
                            lineage_scope="FIELD",
                        )
                    )

        for field in column_lineage_facet.get("dataset") or []:
            if not field.get("name") or not field.get("field"):
                continue
            input_name = str(field.get("name") or "")
            input_table = input_name
            for transformation in field.get("transformations") or []:
                if not isinstance(transformation, dict):
                    continue
                lineage_type = transformation.get("type")
                lineage_subtype = transformation.get("subtype")
                masking = transformation.get("masking")
                transformations.append(
                    TransformationSpec(
                        output_table=output_table,
                        output_attribute="__dataset__",
                        input_attributes=((input_table, str(field.get("field") or "")),),
                        source="openlineage",
                        lineage_type=None if lineage_type is None else str(lineage_type).upper(),
                        lineage_subtype=None if lineage_subtype is None else str(lineage_subtype).upper(),
                        lineage_description=transformation.get("description"),
                        lineage_masking=masking if isinstance(masking, bool) else None,
                        lineage_scope="DATASET",
                    )
                )
    return tuple(transformations)
