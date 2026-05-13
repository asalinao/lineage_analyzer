from __future__ import annotations

import traceback
from dataclasses import asdict
from typing import Any

from .db_introspect import inspect_database
from .dsn import database_dsn, resolve_warehouse_dsn_password
from .graph import LineageGraph
from .models import AttributeChange, ChangeType, EventType, TableSpec, TransformationChange, TransformationSpec
from .openlineage import (
    parse_openlineage_event,
    tables_from_openlianege_event,
    transformations_from_openlienage_event,
)
from .repository import MetadataRepository
from .sql_parser import SqlParser


FINAL_EVENT_TYPES = {
    EventType.COMPLETE,
    EventType.FAIL,
    EventType.FAILED,
    EventType.ABORT,
    EventType.ABORTED,
}


class LineageService:
    def __init__(self, repository: MetadataRepository, sql_parser: SqlParser | None = None) -> None:
        self.repository = repository
        self.sql_parser = sql_parser or SqlParser()

    def process_openlineage_event(self, payload: dict[str, Any]) -> dict[str, int | bool]:
        event = parse_openlineage_event(payload)

        try:
            with self.repository.transaction():
                if event.event_type not in FINAL_EVENT_TYPES:
                    self.repository.save_event(event.run_id, None, event.event_type, event.event_time.isoformat(), payload)
                    return {
                        "model_updated": False,
                        "job_changed": False,
                        "inputs": 0,
                        "outputs": 0,
                        "transformations": 0,
                    }

                inputs, outputs = tables_from_openlianege_event(
                    payload.get("inputs", []),
                    payload.get("outputs", []),
                )
                
                if not outputs:
                    self.repository.save_event(event.run_id, None, event.event_type, event.event_time.isoformat(), payload)
                    return {
                        "model_updated": False,
                        "job_changed": False,
                        "inputs": len(inputs),
                        "outputs": 0,
                        "transformations": 0,
                    }

                input_ids, output_ids, tables_changed = self.update_tables_and_attributes(inputs, outputs)

                job_result = self.repository.upsert_job(event.job, input_ids, output_ids)
                self.repository.save_event(event.run_id, job_result.id, event.event_type, event.event_time.isoformat(), payload)

                transformations = transformations_from_openlienage_event(payload.get("outputs", []))
                if not transformations:
                    transformations = self._extract_transformations_from_sql(payload, inputs, outputs)

                transformations_changed = False
                if transformations:
                    transformations_changed = not self.repository.transformations_match_actual_job(
                        job_result.id,
                        transformations,
                    )
                if transformations_changed:
                    self.repository.replace_transformations(job_result.id, transformations)

                return {
                    "model_updated": tables_changed or job_result.changed or transformations_changed,
                    "job_changed": job_result.changed,
                    "inputs": len(inputs),
                    "outputs": len(outputs),
                    "transformations": len(transformations),
                }
        except Exception:
            print("Failed to process OpenLineage event", flush=True)
            traceback.print_exc()
            raise

    def update_tables_and_attributes(
        self,
        inputs: tuple[TableSpec, ...],
        outputs: tuple[TableSpec, ...],
    ) -> tuple[list[int], list[int], bool]:
        input_ids: list[int] = []
        output_ids: list[int] = []
        changed = False

        queue = [(table, input_ids) for table in inputs] + [(table, output_ids) for table in outputs]
        while queue:
            table, target_ids = queue.pop(0)
            table_changed = not self.repository.table_matches_actual_model(table)
            table_id = self.repository.upsert_table(table)
            target_ids.append(table_id)
            changed = changed or table_changed

        return input_ids, output_ids, changed

    def sync_database_metadata(
        self,
        tables: list[TableSpec],
        namespace: str | None = None,
        schema: str | None = None,
    ) -> dict[str, object]:
        namespace = namespace or (tables[0].namespace if tables else "")
        schema = schema or self._schema_from_tables(tables)
        changed_tables: list[str] = []
        unchanged_tables: list[str] = []

        with self.repository.transaction():
            for table in tables:
                changed = not self.repository.table_matches_actual_model(table)
                self.repository.upsert_table(table)
                if changed:
                    changed_tables.append(table.name)
                else:
                    unchanged_tables.append(table.name)

            removed_tables = self.repository.mark_missing_tables_inactive_scoped(
                (table.name for table in tables),
                namespace,
                schema,
            )

        return {
            "actual_tables": len(tables),
            "changed_tables": changed_tables,
            "unchanged_tables": unchanged_tables,
            "removed_tables": removed_tables,
        }

    def run_sync_schedule(self, schedule_id: int) -> dict[str, object]:
        schedule = self.repository.get_sync_schedule(schedule_id)
        if schedule is None:
            raise ValueError(f"Sync schedule not found: {schedule_id}")
        try:
            source_type = str(schedule.get("source_type") or "postgresql")
            namespace = str(schedule["namespace"])
            schema = str(schedule["schema_name"])
            if source_type in {"postgresql", "greenplum"}:
                dsn = database_dsn(str(schedule["warehouse_dsn"]), namespace)
                inspect_schema = schema
                result_namespace = namespace
                result_schema = schema
            elif source_type == "clickhouse":
                dsn = resolve_warehouse_dsn_password(str(schedule["warehouse_dsn"]))
                inspect_schema = namespace
                result_namespace = namespace
                result_schema = namespace
            elif source_type == "hadoop_spark":
                dsn = resolve_warehouse_dsn_password(str(schedule["warehouse_dsn"]))
                inspect_schema = schema
                result_namespace = schema
                result_schema = schema
            else:
                raise ValueError(f"Unsupported source_type: {source_type}")
            tables = inspect_database(
                source_type,
                dsn,
                schema=inspect_schema,
            )
            result = self.sync_database_metadata(tables, namespace=result_namespace, schema=result_schema)
            self.repository.record_sync_schedule_run(schedule_id, result, error=None)
            print(f"Sync schedule finished: id={schedule_id}", flush=True)
            return result
        except Exception as error:
            self.repository.rollback()
            try:
                self.repository.record_sync_schedule_run(schedule_id, None, error=str(error))
            except Exception:
                print(f"Sync schedule error status was not saved: id={schedule_id}", flush=True)
                traceback.print_exc()
            print(f"Sync schedule failed: id={schedule_id}", flush=True)
            traceback.print_exc()
            raise

    def compare_table_states(self, old_table_id: int, new_table_id: int) -> dict[str, Any]:
        if old_table_id == new_table_id:
            raise ValueError("Cannot compare the same table version")
        old_table = self.repository.get_table_state(old_table_id)
        new_table = self.repository.get_table_state(new_table_id)
        if old_table is None or new_table is None:
            raise LookupError("Table version not found")

        old_attributes = {
            str(row["name"]): row
            for row in self.repository.attributes_by_table_id(old_table_id)
        }
        new_attributes = {
            str(row["name"]): row
            for row in self.repository.attributes_by_table_id(new_table_id)
        }
        table_name = str(new_table["name"])
        changes: list[AttributeChange] = []

        for attribute_name in sorted(set(old_attributes) | set(new_attributes)):
            old_attr = old_attributes.get(attribute_name)
            new_attr = new_attributes.get(attribute_name)
            if old_attr is None and new_attr is not None:
                changes.append(
                    AttributeChange(
                        table_name=table_name,
                        attribute_name=attribute_name,
                        change_type=ChangeType.ADDED,
                        new_type=str(new_attr["data_type"]),
                    )
                )
            elif old_attr is not None and new_attr is None:
                changes.append(
                    AttributeChange(
                        table_name=str(old_table["name"]),
                        attribute_name=attribute_name,
                        change_type=ChangeType.REMOVED,
                        old_type=str(old_attr["data_type"]),
                    )
                )
            elif old_attr is not None and new_attr is not None and old_attr["data_type"] != new_attr["data_type"]:
                changes.append(
                    AttributeChange(
                        table_name=table_name,
                        attribute_name=attribute_name,
                        change_type=ChangeType.MODIFIED,
                        old_type=str(old_attr["data_type"]),
                        new_type=str(new_attr["data_type"]),
                    )
                )

        return self._impact_report(
            source={
                "type": "table",
                "name": table_name,
                "old_version": old_table["version"],
                "new_version": new_table["version"],
            },
            changes=changes,
            transformation_changes=[],
        )

    def compare_job_states(self, old_job_id: int, new_job_id: int) -> dict[str, Any]:
        if old_job_id == new_job_id:
            raise ValueError("Cannot compare the same job version")
        old_job = self.repository.get_job_state(old_job_id)
        new_job = self.repository.get_job_state(new_job_id)
        if old_job is None or new_job is None:
            raise LookupError("Job version not found")

        old_transformations = self.repository.transformations_by_job_id(old_job_id)
        new_transformations = self.repository.transformations_by_job_id(new_job_id)
        old_by_key = {self._transformation_identity(row): row for row in old_transformations}
        new_by_key = {self._transformation_identity(row): row for row in new_transformations}
        changes: list[TransformationChange] = []

        for key in sorted(set(old_by_key) | set(new_by_key)):
            old_item = old_by_key.get(key)
            new_item = new_by_key.get(key)
            item = new_item or old_item or {}
            if old_item is None:
                change_type = ChangeType.ADDED
            elif new_item is None:
                change_type = ChangeType.REMOVED
            elif self._transformation_payload(old_item) != self._transformation_payload(new_item):
                change_type = ChangeType.MODIFIED
            else:
                continue
            changes.append(self._transformation_change(str(new_job["name"]), item, change_type))

        if not changes and old_job.get("sql") != new_job.get("sql"):
            output_points = {
                (row.get("output_table"), row.get("output_attribute"))
                for row in new_transformations
                if row.get("output_table") and row.get("output_attribute")
            }
            for output_table, output_attribute in sorted(output_points):
                changes.append(
                    TransformationChange(
                        job_name=str(new_job["name"]),
                        output_table=str(output_table),
                        output_attribute=str(output_attribute),
                        change_type=ChangeType.LOGIC_CHANGED,
                    )
                )

        start_changes = [
            AttributeChange(
                table_name=str(change.output_table),
                attribute_name=str(change.output_attribute),
                change_type=change.change_type,
            )
            for change in changes
            if change.output_table and change.output_attribute
        ]
        return self._impact_report(
            source={
                "type": "job",
                "name": new_job["name"],
                "old_version": old_job["version"],
                "new_version": new_job["version"],
            },
            changes=start_changes,
            transformation_changes=changes,
            context={"sql": new_job.get("sql")},
        )

    def _schema_from_tables(self, tables: list[TableSpec]) -> str:
        if not tables:
            return ""
        name_parts = tables[0].name.split(".", 1)
        if len(name_parts) != 2:
            return ""
        rest_parts = name_parts[1].split(".", 1)
        return rest_parts[0] if len(rest_parts) == 2 else name_parts[0]

    def _extract_transformations_from_sql(
        self,
        payload: dict[str, Any],
        inputs: tuple[TableSpec, ...],
        outputs: tuple[TableSpec, ...],
    ) -> tuple[TransformationSpec, ...]:
        sql: str | None = None
        dialect: str | None = None
        facets = payload.get("facets") or {}
        job_facets = ((payload.get("job") or {}).get("facets") or {})
        for source in (facets, job_facets):
            sql_facet = source.get("sql") or source.get("sqlQuery")
            if isinstance(sql_facet, dict):
                query = sql_facet.get("query") or sql_facet.get("sql")
                if query:
                    sql = str(query)
                    dialect = sql_facet.get("dialect")
                    break
            source_code = source.get("sourceCode")
            if isinstance(source_code, dict) and source_code.get("sourceCode"):
                sql = str(source_code["sourceCode"])
                dialect = source_code.get("language")
                break

        if not sql:
            return ()
        if not self.sql_parser.is_supported(dialect):
            print(f"SQL dialect is not supported: {dialect}", flush=True)
            return ()
        return self.sql_parser.extract_transformations(sql, str(dialect).lower(), inputs, outputs)

    def _impact_report(
        self,
        source: dict[str, Any],
        changes: list[AttributeChange],
        transformation_changes: list[TransformationChange],
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        paths = LineageGraph(self.repository).impact_paths_from_attributes(changes)
        changes_by_source = {
            (change.table_name, change.attribute_name): change.change_type.value
            for change in changes
        }
        affected: dict[str, dict[str, Any]] = {}
        if source.get("type") == "job":
            for change in changes:
                table = affected.setdefault(
                    change.table_name,
                    {
                        "name": change.table_name,
                        "attributes": {},
                        "distance": 0,
                    },
                )
                table["attributes"][change.attribute_name] = change.change_type.value

        for path in paths:
            table = affected.setdefault(
                path.affected_table,
                {
                    "name": path.affected_table,
                    "attributes": {},
                    "distance": path.depth,
                },
            )
            table["attributes"][path.affected_attribute] = changes_by_source.get(
                (path.source_table, path.source_attribute)
            )
            table["distance"] = min(int(table["distance"]), path.depth)

        affected_tables = [
            {
                "name": table["name"],
                "affected_attributes": len(table["attributes"]),
                "attributes": [
                    {"name": name, "change_type": change_type}
                    for name, change_type in sorted(table["attributes"].items())
                ],
                "distance": table["distance"],
            }
            for table in sorted(affected.values(), key=lambda item: (item["distance"], item["name"]))
            if source.get("type") == "job" or int(table["distance"]) > 0
        ]
        report = {
            "source": source,
            "changes": [
                self._public_transformation_change(change)
                for change in transformation_changes
            ] or [
                self._public_attribute_change(change)
                for change in changes
            ],
            "affected_tables": affected_tables,
        }
        if context:
            report["context"] = context
        return report

    def _transformation_change(
        self,
        job_name: str,
        row: dict[str, Any],
        change_type: ChangeType,
    ) -> TransformationChange:
        return TransformationChange(
            job_name=job_name,
            output_table=None if row.get("output_table") is None else str(row.get("output_table")),
            output_attribute=None if row.get("output_attribute") is None else str(row.get("output_attribute")),
            change_type=change_type,
            input_table=None if row.get("input_table") is None else str(row.get("input_table")),
            input_attribute=None if row.get("input_attribute") is None else str(row.get("input_attribute")),
            expression=None if row.get("expression") is None else str(row.get("expression")),
        )

    def _transformation_identity(self, row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            row.get("input_table"),
            row.get("input_attribute"),
            row.get("output_table"),
            row.get("output_attribute"),
        )

    def _transformation_payload(self, row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            row.get("lineage_type"),
            row.get("lineage_subtype"),
            row.get("lineage_description"),
            row.get("lineage_masking"),
            row.get("lineage_scope"),
            row.get("source"),
            row.get("expression"),
        )

    def _public_attribute_change(self, change: AttributeChange) -> dict[str, Any]:
        payload = asdict(change)
        payload["change_type"] = change.change_type.value
        payload["attribute"] = change.attribute_name
        return payload

    def _public_transformation_change(self, change: TransformationChange) -> dict[str, Any]:
        payload = asdict(change)
        payload["change_type"] = change.change_type.value
        return payload
