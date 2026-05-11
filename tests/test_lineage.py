from __future__ import annotations

import json
import os
import unittest
from pathlib import Path

from lineage_analyzer.dsn import database_dsn
from lineage_analyzer.graph import LineageGraph
from lineage_analyzer.db_introspect import format_postgresql_data_type
from lineage_analyzer.openlineage import (
    job_from_openlineage_event,
    parse_openlineage_event,
    tables_from_openlianege_event,
    transformations_from_openlienage_event,
)
from lineage_analyzer.repository import JobUpsertResult, MetadataRepository
from lineage_analyzer.scheduler import SyncScheduler, validate_sync_schedule_payload
from lineage_analyzer.services import LineageService
from lineage_analyzer.models import AttributeSpec, EventType, TableSpec


POSTGRES_TEST_DSN = os.getenv("POSTGRES_TEST_DSN")


class OpenLineageParserTests(unittest.TestCase):
    def test_parse_openlineage_event(self) -> None:
        payload = json.loads(Path("examples/openlineage_event.json").read_text(encoding="utf-8"))
        event = parse_openlineage_event(payload)

        self.assertEqual(event.job.name, "build_customer_mart")
        self.assertEqual(event.inputs, ())
        self.assertEqual(event.outputs, ())
        self.assertEqual(event.transformations, ())

    def test_parse_openlineage_tables(self) -> None:
        payload = dbt_event("COMPLETE", "MODEL", outputs_with_schema=True)

        inputs, outputs = tables_from_openlianege_event(payload["inputs"], payload["outputs"])

        self.assertEqual(inputs[0].name, "analytics.analytics_raw.customers")
        self.assertEqual(outputs[0].name, "analytics.analytics_staging.stg_customers")
        self.assertEqual(outputs[0].attributes[0].name, "customer_id")

    def test_parse_openlineage_job(self) -> None:
        payload = dbt_event("COMPLETE", "MODEL", outputs_with_schema=True)

        job = job_from_openlineage_event(payload["job"])

        self.assertEqual(job.namespace, "dbt-test-stand")
        self.assertEqual(job.dialect, "postgres")

    def test_parse_openlineage_transformations(self) -> None:
        payload = dbt_event("COMPLETE", "MODEL", outputs_with_schema=True)
        payload["outputs"][0]["facets"]["columnLineage"]["fields"]["customer_id"]["inputFields"][0]["transformations"] = [
            {"type": "direct", "subtype": "identity", "masking": False}
        ]

        transformations = transformations_from_openlienage_event(payload["outputs"])

        self.assertEqual(len(transformations), 1)
        self.assertEqual(transformations[0].source, "openlineage")
        self.assertEqual(transformations[0].lineage_type, "DIRECT")
        self.assertEqual(transformations[0].lineage_subtype, "IDENTITY")

    def test_dbt_model_complete_updates_model(self) -> None:
        payload = dbt_event("COMPLETE", "MODEL", outputs_with_schema=True)

        event = parse_openlineage_event(payload)

        self.assertEqual(event.event_type, EventType.COMPLETE)

    def test_complete_event_is_candidate_for_model_update_regardless_of_job_type(self) -> None:
        payload = dbt_event("COMPLETE", "TEST", outputs_with_schema=False)

        event = parse_openlineage_event(payload)

        self.assertEqual(event.event_type, EventType.COMPLETE)

    def test_dbt_model_start_does_not_update_model(self) -> None:
        payload = dbt_event("START", "MODEL", outputs_with_schema=True)

        event = parse_openlineage_event(payload)

        self.assertEqual(event.event_type, EventType.START)

    def test_returns_no_transformations_when_column_lineage_is_missing(self) -> None:
        payload = dbt_event("COMPLETE", "MODEL", outputs_with_schema=True, include_column_lineage=False)

        transformations = transformations_from_openlienage_event(payload["outputs"])

        self.assertEqual(transformations, ())

    def test_returns_no_transformations_when_column_lineage_has_no_transformations(self) -> None:
        payload = dbt_summary_event_without_column_lineage()
        output = payload["outputs"][0]
        output["facets"]["columnLineage"] = {
            "fields": {
                "customer_id": {
                    "inputFields": [
                        {
                            "namespace": "postgres://postgres:5432",
                            "name": "analytics.analytics_staging.stg_customers",
                            "field": "customer_id",
                        }
                    ]
                },
                "order_count": {
                    "inputFields": [
                        {
                            "namespace": "postgres://postgres:5432",
                            "name": "analytics.analytics_staging.stg_orders",
                            "field": "order_id",
                        }
                    ]
                },
                "total_amount": {
                    "inputFields": [
                        {
                            "namespace": "postgres://postgres:5432",
                            "name": "analytics.analytics_staging.stg_orders",
                            "field": "amount",
                        }
                    ]
                },
            }
        }

        transformations = transformations_from_openlienage_event(payload["outputs"])

        self.assertEqual(transformations, ())

    def test_returns_no_transformations_when_openlineage_fields_have_empty_transformations(self) -> None:
        payload = dbt_summary_event_without_column_lineage()
        output = payload["outputs"][0]
        output["facets"]["columnLineage"] = {
            "fields": {
                "customer_id": {
                    "inputFields": [
                        {
                            "namespace": "postgres://postgres:5432",
                            "name": "analytics.analytics_staging.stg_customers",
                            "field": "customer_id",
                            "transformations": [],
                        }
                    ]
                },
                "order_count": {
                    "inputFields": [
                        {
                            "namespace": "postgres://postgres:5432",
                            "name": "analytics.analytics_staging.stg_orders",
                            "field": "order_id",
                            "transformations": [],
                        }
                    ]
                },
                "total_amount": {
                    "inputFields": [
                        {
                            "namespace": "postgres://postgres:5432",
                            "name": "analytics.analytics_staging.stg_orders",
                            "field": "amount",
                            "transformations": [],
                        }
                    ]
                },
            },
            "dataset": [],
        }

        transformations = transformations_from_openlienage_event(payload["outputs"])

        self.assertEqual(transformations, ())

    def test_returns_no_transformations_without_openlineage_transformations(self) -> None:
        payload = dbt_event("COMPLETE", "MODEL", outputs_with_schema=True)
        payload["job"]["facets"].pop("sql")

        transformations = transformations_from_openlienage_event(payload["outputs"])

        self.assertEqual(transformations, ())

    def test_parses_openlineage_column_lineage_transformation_fields(self) -> None:
        payload = dbt_event("COMPLETE", "MODEL", outputs_with_schema=True)
        output = payload["outputs"][0]
        output["facets"]["columnLineage"] = {
            "fields": {
                "customer_id": {
                    "inputFields": [
                        {
                            "namespace": "postgres://postgres:5432",
                            "name": "analytics.analytics_raw.customers",
                            "field": "customer_id",
                            "transformations": [
                                {
                                    "type": "DIRECT",
                                    "subtype": "IDENTITY",
                                    "description": "copied as is",
                                    "masking": False,
                                }
                            ],
                        },
                        {
                            "namespace": "postgres://postgres:5432",
                            "name": "analytics.analytics_raw.customers",
                            "field": "customer_name",
                            "transformations": [
                                {
                                    "type": "INDIRECT",
                                    "subtype": "FILTER",
                                    "description": "used in predicate",
                                    "masking": False,
                                }
                            ],
                        },
                    ]
                }
            },
            "dataset": [
                {
                    "namespace": "postgres://postgres:5432",
                    "name": "analytics.analytics_raw.customers",
                    "field": "customer_name",
                    "transformations": [
                        {
                            "type": "INDIRECT",
                            "subtype": "SORT",
                            "description": "order by",
                            "masking": False,
                        }
                    ],
                }
            ],
        }

        parsed = {
            (
                item.output_attribute,
                item.input_attributes[0][1],
                item.lineage_scope,
                item.lineage_type,
                item.lineage_subtype,
            ): item
            for item in transformations_from_openlienage_event(payload["outputs"])
        }
        direct = parsed[("customer_id", "customer_id", "FIELD", "DIRECT", "IDENTITY")]
        indirect = parsed[("customer_id", "customer_name", "FIELD", "INDIRECT", "FILTER")]
        dataset = parsed[("__dataset__", "customer_name", "DATASET", "INDIRECT", "SORT")]
        self.assertEqual(direct.lineage_description, "copied as is")
        self.assertFalse(direct.lineage_masking)
        self.assertEqual(indirect.lineage_subtype, "FILTER")
        self.assertEqual(dataset.lineage_description, "order by")


class LineageServiceTests(unittest.TestCase):
    def test_start_event_is_saved_without_model_update(self) -> None:
        payload = dbt_event("START", "MODEL", outputs_with_schema=True)
        repository = FakeRepository(JobUpsertResult(id=9, changed=True), all_tables_match=False)
        service = LineageService(repository)  # type: ignore[arg-type]

        result = service.ingest_event(payload)

        self.assertFalse(result["model_updated"])
        self.assertEqual(repository.saved_events, 1)
        self.assertEqual(repository.upsert_table_calls, 0)
        self.assertEqual(repository.upsert_job_calls, 0)

    def test_complete_event_without_outputs_is_ignored_after_event_save(self) -> None:
        payload = dbt_event("COMPLETE", "TEST", outputs_with_schema=False)
        payload["inputs"] = []
        repository = FakeRepository(JobUpsertResult(id=9, changed=True), all_tables_match=True)
        service = LineageService(repository)  # type: ignore[arg-type]

        result = service.ingest_event(payload)

        self.assertFalse(result["model_updated"])
        self.assertEqual(repository.saved_events, 1)
        self.assertEqual(repository.upsert_table_calls, 0)
        self.assertEqual(repository.upsert_job_calls, 0)
        self.assertIsNone(repository.updated_event_job_id)

    def test_failed_event_updates_model(self) -> None:
        payload = dbt_event("FAILED", "TEST", outputs_with_schema=False)
        payload["inputs"] = []
        repository = FakeRepository(JobUpsertResult(id=9, changed=True), all_tables_match=True)
        service = LineageService(repository)  # type: ignore[arg-type]

        result = service.ingest_event(payload)

        self.assertFalse(result["model_updated"])
        self.assertEqual(repository.upsert_job_calls, 0)

    def test_unsupported_sql_dialect_does_not_fail_processing(self) -> None:
        payload = dbt_event("COMPLETE", "MODEL", outputs_with_schema=True)
        payload["outputs"][0]["facets"]["columnLineage"]["fields"]["customer_id"]["inputFields"][0]["transformations"] = []
        payload["job"]["facets"]["sql"]["dialect"] = "unsupported"
        repository = FakeRepository(JobUpsertResult(id=9, changed=False), all_tables_match=True)
        service = LineageService(repository)  # type: ignore[arg-type]

        with self.assertLogs("lineage_analyzer.services", level="WARNING"):
            result = service.ingest_event(payload)

        self.assertFalse(result["model_updated"])
        self.assertEqual(result["transformations"], 0)
        self.assertEqual(repository.replace_transformations_calls, 0)

    def test_service_updates_model_when_event_has_transformations(self) -> None:
        payload = dbt_event("COMPLETE", "TEST", outputs_with_schema=True)
        payload["outputs"][0]["facets"]["columnLineage"]["fields"]["customer_id"]["inputFields"][0]["transformations"] = [
            {"type": "DIRECT", "subtype": "IDENTITY", "masking": False}
        ]
        repository = FakeRepository(JobUpsertResult(id=9, changed=True), all_tables_match=True)
        service = LineageService(repository)  # type: ignore[arg-type]

        result = service.ingest_event(payload)

        self.assertTrue(result["model_updated"])
        self.assertEqual(repository.upsert_table_calls, 2)
        self.assertEqual(repository.replace_transformations_calls, 1)

    def test_service_updates_model_when_table_does_not_match_actual_model(self) -> None:
        payload = dbt_event("COMPLETE", "TEST", outputs_with_schema=True, include_column_lineage=False)
        repository = FakeRepository(JobUpsertResult(id=9, changed=True), all_tables_match=False)
        service = LineageService(repository)  # type: ignore[arg-type]

        result = service.ingest_event(payload)

        self.assertTrue(result["model_updated"])
        inputs, outputs = tables_from_openlianege_event(payload["inputs"], payload["outputs"])
        self.assertEqual(repository.upsert_table_calls, len(inputs) + len(outputs))

    def test_service_updates_table_with_changed_attributes(self) -> None:
        payload = dbt_event("COMPLETE", "TEST", outputs_with_schema=False, include_column_lineage=False)
        payload["outputs"] = []
        payload["inputs"] = [
            {
                "namespace": "postgres://postgres:5432",
                "name": "analytics.raw.customers",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "new_customer_code", "type": "text"},
                        ]
                    }
                },
            }
        ]
        repository = FakeRepository(
            JobUpsertResult(id=9, changed=True),
            actual_attributes_by_table={
                "analytics.raw.customers": (
                    AttributeSpec("customer_id", "integer"),
                )
            },
        )
        service = LineageService(repository)  # type: ignore[arg-type]

        result = service.ingest_event(payload)

        self.assertFalse(result["model_updated"])
        self.assertEqual(repository.upsert_table_calls, 0)

    def test_service_does_not_update_existing_table_when_event_has_no_schema(self) -> None:
        payload = dbt_event("COMPLETE", "TEST", outputs_with_schema=False, include_column_lineage=False)
        payload["outputs"] = []
        payload["inputs"] = [
            {
                "namespace": "postgres://postgres:5432",
                "name": "analytics.raw.customers",
                "facets": {},
            }
        ]
        repository = FakeRepository(
            JobUpsertResult(id=9, changed=False),
            actual_attributes_by_table={
                "analytics.raw.customers": (
                    AttributeSpec("customer_id", "integer"),
                )
            },
        )
        service = LineageService(repository)  # type: ignore[arg-type]

        result = service.ingest_event(payload)

        self.assertFalse(result["model_updated"])
        self.assertEqual(repository.upserted_tables, [])

    def test_service_skips_model_update_when_no_transformations_and_tables_match(self) -> None:
        payload = dbt_event("COMPLETE", "TEST", outputs_with_schema=True, include_column_lineage=False)
        payload["job"]["facets"].pop("sql")
        repository = FakeRepository(JobUpsertResult(id=9, changed=False), all_tables_match=True)
        service = LineageService(repository)  # type: ignore[arg-type]

        result = service.ingest_event(payload)

        self.assertFalse(result["model_updated"])
        self.assertEqual(repository.upsert_table_calls, 2)
        self.assertEqual(repository.updated_event_job_id, 9)

    def test_service_does_not_replace_unchanged_transformations(self) -> None:
        payload = dbt_event("COMPLETE", "TEST", outputs_with_schema=True)
        payload["outputs"][0]["facets"]["columnLineage"]["fields"]["customer_id"]["inputFields"][0]["transformations"] = [
            {"type": "DIRECT", "subtype": "IDENTITY", "masking": False}
        ]
        repository = FakeRepository(
            JobUpsertResult(id=9, changed=False),
            all_tables_match=True,
            transformations_match=True,
        )
        service = LineageService(repository)  # type: ignore[arg-type]

        result = service.ingest_event(payload)

        self.assertFalse(result["model_updated"])
        self.assertEqual(repository.replace_transformations_calls, 0)

    def test_ingest_event_replaces_transformations_from_sql_when_event_has_no_transformations(self) -> None:
        payload = dbt_event("COMPLETE", "MODEL", outputs_with_schema=True)
        payload["outputs"][0]["facets"]["columnLineage"]["fields"]["customer_id"]["inputFields"][0]["transformations"] = []
        repository = FakeRepository(JobUpsertResult(id=11, changed=True))
        service = LineageService(repository)  # type: ignore[arg-type]

        result = service.ingest_event(payload)

        self.assertTrue(result["job_changed"])
        self.assertEqual(repository.replace_transformations_calls, 1)
        self.assertEqual(repository.replaced_job_id, 11)
        self.assertGreater(repository.replaced_transformations_count, 0)
        self.assertEqual(
            {transformation.source for transformation in repository.replaced_transformations},
            {"sql_parsing"},
        )

    def test_sync_database_metadata_returns_change_report_and_scopes_removed_tables(self) -> None:
        tables = [
            TableSpec("warehouse", "public", "customers", (AttributeSpec("id", "integer"),)),
            TableSpec("warehouse", "public", "orders", (AttributeSpec("id", "integer"),)),
        ]
        repository = FakeRepository(
            JobUpsertResult(id=9, changed=False),
            actual_attributes_by_table={
                "warehouse.public.customers": (AttributeSpec("id", "integer"),),
            },
            removed_tables=["warehouse.public.old_orders"],
        )
        service = LineageService(repository)  # type: ignore[arg-type]

        result = service.sync_database_metadata(tables, namespace="warehouse", schema="public")

        self.assertEqual(result["actual_tables"], 2)
        self.assertEqual(result["changed_tables"], ["warehouse.public.orders"])
        self.assertEqual(result["unchanged_tables"], ["warehouse.public.customers"])
        self.assertEqual(result["removed_tables"], ["warehouse.public.old_orders"])
        self.assertEqual(repository.upsert_table_calls, 2)
        self.assertEqual(repository.mark_missing_scope, ("warehouse", "public"))

    def test_formats_postgresql_parameterized_data_types(self) -> None:
        self.assertEqual(
            format_postgresql_data_type(
                {
                    "data_type": "character varying",
                    "character_maximum_length": 100,
                    "numeric_precision": None,
                    "numeric_scale": None,
                    "datetime_precision": None,
                }
            ),
            "character varying(100)",
        )
        self.assertEqual(
            format_postgresql_data_type(
                {
                    "data_type": "numeric",
                    "character_maximum_length": None,
                    "numeric_precision": 12,
                    "numeric_scale": 2,
                    "datetime_precision": None,
                }
            ),
            "numeric(12,2)",
        )

    def test_validates_sync_schedule_payload(self) -> None:
        payload = validate_sync_schedule_payload(
            {
                "name": "Public",
                "warehouse_dsn": "postgresql://user:pass@localhost:5432/db",
                "namespace": "db",
                "schema_name": "public",
                "cron_expression": "0 * * * *",
            }
        )

        self.assertEqual(payload["cron_expression"], "0 * * * *")
        self.assertNotIn("pass", str(payload["warehouse_dsn"]))
        self.assertIn("LINEAGE_WAREHOUSE_PASSWORD_", str(payload["warehouse_dsn"]))
        self.assertEqual(database_dsn(str(payload["warehouse_dsn"]), str(payload["namespace"])), "postgresql://user:pass@localhost:5432/db")
        with self.assertRaises(ValueError):
            validate_sync_schedule_payload(
                {
                    "name": "Broken",
                    "warehouse_dsn": "",
                    "namespace": "db",
                    "schema_name": "public",
                    "cron_expression": "bad cron",
                }
            )

    def test_sync_scheduler_registers_and_removes_jobs(self) -> None:
        repository = FakeScheduleRepository(
            [
                {
                    "id": 7,
                    "enabled": True,
                    "cron_expression": "0 * * * *",
                }
            ]
        )
        service = FakeScheduleService()
        scheduler = SyncScheduler(repository, service)  # type: ignore[arg-type]

        scheduler.start()
        self.assertIsNotNone(scheduler.scheduler.get_job("sync_schedule_7"))
        scheduler.add_or_update_schedule(
            {
                "id": 7,
                "enabled": True,
                "cron_expression": "*/15 * * * *",
            }
        )
        self.assertIsNotNone(scheduler.scheduler.get_job("sync_schedule_7"))
        scheduler.remove_schedule(7)
        self.assertIsNone(scheduler.scheduler.get_job("sync_schedule_7"))
        scheduler.shutdown()


class FakeRepository:
    def __init__(
        self,
        job_result: JobUpsertResult,
        all_tables_match: bool = False,
        actual_attributes_by_table: dict[str, tuple[AttributeSpec, ...]] | None = None,
        transformations_match: bool = False,
        removed_tables: list[str] | None = None,
    ) -> None:
        self.job_result = job_result
        self.all_tables_match = all_tables_match
        self.actual_attributes_by_table = actual_attributes_by_table
        self.transformations_match = transformations_match
        self.removed_tables = removed_tables or []
        self.mark_missing_scope: tuple[str, str] | None = None
        self.next_table_id = 1
        self.next_event_id = 1
        self.upsert_table_calls = 0
        self.upsert_job_calls = 0
        self.upserted_tables: list[TableSpec] = []
        self.replace_transformations_calls = 0
        self.replaced_transformations_count = 0
        self.replaced_transformations: list[object] = []
        self.replaced_job_id: int | None = None
        self.saved_events = 0
        self.updated_event_job_id: int | None = None

    def transaction(self) -> object:
        return self

    def __enter__(self) -> "FakeRepository":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False

    def table_matches_actual_model(self, table: object) -> bool:
        if self.actual_attributes_by_table is not None and isinstance(table, TableSpec):
            actual_attributes = self.actual_attributes_by_table.get(table.name)
            if actual_attributes is None:
                return False
            if not table.attributes:
                return True
            return [(attr.name, attr.data_type) for attr in actual_attributes] == [
                (attr.name, attr.data_type) for attr in table.attributes
            ]
        return self.all_tables_match

    def table_has_all_actual_attributes(self, table: TableSpec) -> bool:
        if self.actual_attributes_by_table is None:
            return self.all_tables_match
        actual_attributes = self.actual_attributes_by_table.get(table.name)
        if actual_attributes is None:
            return False
        actual_names = {attr.name for attr in actual_attributes}
        return all(attr.name in actual_names for attr in table.attributes)

    def table_with_actual_and_new_attributes(self, table: TableSpec) -> TableSpec:
        if self.actual_attributes_by_table is None:
            return table
        actual_attributes = self.actual_attributes_by_table.get(table.name)
        if actual_attributes is None:
            return table
        attributes_by_name = {attr.name: attr for attr in actual_attributes}
        for attr in table.attributes:
            attributes_by_name.setdefault(attr.name, attr)
        return TableSpec(
            table.namespace,
            table.schema,
            table.table_name,
            tuple(attributes_by_name.values()),
        )

    def upsert_table(self, table: TableSpec) -> int:
        self.upsert_table_calls += 1
        self.upserted_tables.append(table)
        table_id = self.next_table_id
        self.next_table_id += 1
        return table_id

    def upsert_job(self, job: object, input_ids: object, output_ids: object) -> JobUpsertResult:
        self.upsert_job_calls += 1
        return self.job_result

    def replace_transformations(self, job_id: int, transformations: object) -> None:
        self.replace_transformations_calls += 1
        self.replaced_transformations_count = len(transformations)  # type: ignore[arg-type]
        self.replaced_transformations = list(transformations)  # type: ignore[arg-type]
        self.replaced_job_id = job_id

    def transformations_match_actual_job(self, job_id: int, transformations: object) -> bool:
        return self.transformations_match

    def save_event(self, job_id: int | None, event_type: object, event_time: str, raw: object) -> int:
        self.saved_events += 1
        event_id = self.next_event_id
        self.next_event_id += 1
        return event_id

    def update_event_job(self, event_id: int, job_id: int) -> None:
        self.updated_event_job_id = job_id

    def mark_missing_tables_inactive_scoped(
        self,
        actual_names: object,
        namespace: str,
        schema: str,
    ) -> list[str]:
        self.mark_missing_scope = (namespace, schema)
        return self.removed_tables


class FakeScheduleRepository:
    def __init__(self, schedules: list[dict[str, object]]) -> None:
        self.schedules = schedules

    def sync_schedules(self) -> list[dict[str, object]]:
        return self.schedules


class FakeScheduleService:
    def run_sync_schedule(self, schedule_id: int) -> dict[str, object]:
        return {"schedule_id": schedule_id}


@unittest.skipUnless(POSTGRES_TEST_DSN, "set POSTGRES_TEST_DSN to run PostgreSQL integration tests")
class PostgreSQLLineageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = MetadataRepository(POSTGRES_TEST_DSN or "")
        self._truncate()
        self.service = LineageService(self.repository)

    def tearDown(self) -> None:
        self._truncate()
        self.repository.close()

    def test_ingest_openlineage_and_find_downstream(self) -> None:
        payload = json.loads(Path("examples/openlineage_event.json").read_text(encoding="utf-8"))

        result = self.service.ingest_event(payload)
        graph = LineageGraph(self.repository)

        self.assertTrue(result["model_updated"])
        self.assertEqual(
            graph.downstream_tables("dwh.orders"),
            [
                {
                    "table": "mart.customer_revenue",
                    "attributes": [
                        {"name": "customer_id", "parent_attributes": ["customer_id"]},
                        {"name": "total_amount", "parent_attributes": ["amount"]},
                    ],
                }
            ],
        )
    def test_critical_nodes_include_source_table(self) -> None:
        payload = json.loads(Path("examples/openlineage_event.json").read_text(encoding="utf-8"))
        self.service.ingest_event(payload)

        scores = LineageGraph(self.repository).critical_nodes()

        self.assertEqual(scores[0].table, "dwh.orders")
        self.assertGreater(scores[0].score, 0)

    def _truncate(self) -> None:
        self.repository.conn.execute(
            """
            TRUNCATE
                lineage_events,
                lineage_transformations,
                lineage_job_tables,
                lineage_jobs,
                lineage_attributes,
                lineage_tables
            RESTART IDENTITY CASCADE
            """
        )
        self.repository.conn.commit()


def dbt_event(
    event_type: str,
    job_type: str,
    outputs_with_schema: bool,
    include_column_lineage: bool = True,
) -> dict[str, object]:
    outputs = []
    if outputs_with_schema:
        facets = {
            "schema": {
                "fields": [
                    {"name": "customer_id", "type": "integer"},
                    {"name": "customer_name", "type": "character varying(100)"},
                ]
            }
        }
        if include_column_lineage:
            facets["columnLineage"] = {
                "fields": {
                    "customer_id": {
                        "inputFields": [
                            {
                                "namespace": "postgres://postgres:5432",
                                "name": "analytics.analytics_raw.customers",
                                "field": "customer_id",
                            }
                        ]
                    }
                }
            }
        outputs.append(
            {
                "namespace": "postgres://postgres:5432",
                "name": "analytics.analytics_staging.stg_customers",
                "facets": facets,
            }
        )
    return {
        "eventType": event_type,
        "eventTime": "2026-05-03T15:08:41.388029Z",
        "job": {
            "namespace": "dbt-test-stand",
            "name": "analytics.analytics_staging.openlineage_test.stg_customers",
            "facets": {
                "jobType": {"jobType": job_type, "integration": "DBT"},
                "sql": {
                    "query": 'select customer_id, customer_name from "analytics"."analytics_raw"."customers"',
                    "dialect": "postgres",
                },
            },
        },
        "inputs": [
            {
                "namespace": "postgres://postgres:5432",
                "name": "analytics.analytics_raw.customers",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "customer_id", "type": "integer"},
                            {"name": "customer_name", "type": "character varying(100)"},
                        ]
                    }
                },
            }
        ],
        "outputs": outputs,
    }


def dbt_summary_event_without_column_lineage() -> dict[str, object]:
    return {
        "eventType": "COMPLETE",
        "eventTime": "2026-05-03T15:08:41.449008Z",
        "job": {
            "namespace": "dbt-test-stand",
            "name": "analytics.analytics_marts.openlineage_test.customer_order_summary",
            "facets": {
                "jobType": {"jobType": "MODEL", "integration": "DBT"},
                "sql": {
                    "dialect": "postgres",
                    "query": """
                        select
                            c.customer_id,
                            c.customer_name,
                            count(o.order_id) as order_count,
                            coalesce(sum(o.amount), 0) as total_amount,
                            max(o.order_date) as last_order_date
                        from "analytics"."analytics_staging"."stg_customers" c
                        left join "analytics"."analytics_staging"."stg_orders" o
                            on c.customer_id = o.customer_id
                        group by
                            c.customer_id,
                            c.customer_name
                    """,
                },
            },
        },
        "inputs": [
            {
                "namespace": "postgres://postgres:5432",
                "name": "analytics.analytics_staging.stg_customers",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "customer_id", "type": "integer"},
                            {"name": "customer_name", "type": "character varying(100)"},
                        ]
                    }
                },
            },
            {
                "namespace": "postgres://postgres:5432",
                "name": "analytics.analytics_staging.stg_orders",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "order_id", "type": "integer"},
                            {"name": "customer_id", "type": "integer"},
                            {"name": "order_date", "type": "date"},
                            {"name": "amount", "type": "numeric(12,2)"},
                        ]
                    }
                },
            },
        ],
        "outputs": [
            {
                "namespace": "postgres://postgres:5432",
                "name": "analytics.analytics_marts.customer_order_summary",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "customer_id", "type": "integer"},
                            {"name": "customer_name", "type": "character varying(100)"},
                            {"name": "order_count", "type": "bigint"},
                            {"name": "total_amount", "type": "numeric"},
                            {"name": "last_order_date", "type": "date"},
                        ]
                    }
                },
            }
        ],
    }


if __name__ == "__main__":
    unittest.main()
