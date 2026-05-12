from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import threading
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .models import DatasetRole, EventType, JobSpec, TableSpec, TransformationSpec


TABLE_NAME_SQL = "namespace || '.' || schema || '.' || table_name"
USER_ROLES = {"data_engineer", "data_analyst"}
PASSWORD_ITERATIONS = 210_000
SESSION_TTL_DAYS = 7


@dataclass(frozen=True, slots=True)
class JobUpsertResult:
    id: int
    changed: bool


class MetadataRepository:
    def __init__(self, dsn: str) -> None:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ModuleNotFoundError as error:
            raise RuntimeError(
                "PostgreSQL storage requires psycopg. Install project dependencies with "
                "`python3 -m pip install -e .` or `python3 -m pip install psycopg[binary]`."
            ) from error

        self.dsn = dsn
        self._lock = threading.RLock()
        self._transaction_depth = 0
        self.conn = psycopg.connect(dsn, autocommit=False, row_factory=dict_row)
        self.migrate()

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def transaction(self):
        with self._lock:
            self._transaction_depth += 1
            try:
                yield
            except Exception:
                self.conn.rollback()
                raise
            else:
                if self._transaction_depth == 1:
                    self.conn.commit()
            finally:
                self._transaction_depth -= 1

    def rollback(self) -> None:
        with self._lock:
            self.conn.rollback()
            self._transaction_depth = 0

    def migrate(self) -> None:
        with self._lock:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lineage_tables (
                    id BIGSERIAL PRIMARY KEY,
                    namespace TEXT NOT NULL,
                    schema TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    is_actual BOOLEAN NOT NULL DEFAULT TRUE,
                    valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    valid_to TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(namespace, schema, table_name, version)
                );
                CREATE TABLE IF NOT EXISTS lineage_attributes (
                    id BIGSERIAL PRIMARY KEY,
                    table_id BIGINT NOT NULL REFERENCES lineage_tables(id),
                    name TEXT NOT NULL,
                    data_type TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    is_actual BOOLEAN NOT NULL DEFAULT TRUE,
                    valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    valid_to TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(table_id, name)
                );
                CREATE TABLE IF NOT EXISTS lineage_jobs (
                    id BIGSERIAL PRIMARY KEY,
                    namespace TEXT NOT NULL,
                    name TEXT NOT NULL,
                    sql TEXT,
                    dialect TEXT,
                    version INTEGER NOT NULL,
                    is_actual BOOLEAN NOT NULL DEFAULT TRUE,
                    valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    valid_to TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(name, version)
                );
                CREATE TABLE IF NOT EXISTS lineage_job_tables (
                    id BIGSERIAL PRIMARY KEY,
                    job_id BIGINT NOT NULL REFERENCES lineage_jobs(id),
                    table_id BIGINT NOT NULL REFERENCES lineage_tables(id),
                    role TEXT NOT NULL,
                    is_actual BOOLEAN NOT NULL DEFAULT TRUE,
                    valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    valid_to TIMESTAMPTZ,
                    UNIQUE(job_id, table_id, role)
                );
                CREATE TABLE IF NOT EXISTS lineage_transformations (
                    id BIGSERIAL PRIMARY KEY,
                    job_id BIGINT NOT NULL REFERENCES lineage_jobs(id),
                    input_attribute_id BIGINT REFERENCES lineage_attributes(id),
                    output_attribute_id BIGINT REFERENCES lineage_attributes(id),
                    lineage_type TEXT,
                    lineage_subtype TEXT,
                    lineage_description TEXT,
                    lineage_masking BOOLEAN,
                    lineage_scope TEXT NOT NULL DEFAULT 'FIELD',
                    source TEXT NOT NULL DEFAULT 'openlineage',
                    expression TEXT,
                    is_actual BOOLEAN NOT NULL DEFAULT TRUE,
                    valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    valid_to TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS lineage_events (
                    id BIGSERIAL PRIMARY KEY,
                    job_id BIGINT REFERENCES lineage_jobs(id),
                    event_type TEXT NOT NULL,
                    event_time TIMESTAMPTZ NOT NULL,
                    raw_json JSONB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS lineage_sync_schedules (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    source_type TEXT NOT NULL DEFAULT 'postgresql',
                    warehouse_dsn TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    schema_name TEXT NOT NULL,
                    cron_expression TEXT,
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    last_run_at TIMESTAMPTZ,
                    last_success_at TIMESTAMPTZ,
                    last_error TEXT,
                    last_result JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS lineage_users (
                    id BIGSERIAL PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    role TEXT NOT NULL CHECK (role IN ('data_engineer', 'data_analyst')),
                    password_hash TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS lineage_sessions (
                    token TEXT PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES lineage_users(id) ON DELETE CASCADE,
                    expires_at TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_lineage_tables_actual
                    ON lineage_tables(namespace, schema, table_name) WHERE is_actual;
                CREATE INDEX IF NOT EXISTS idx_lineage_attributes_actual
                    ON lineage_attributes(table_id, name) WHERE is_actual;
                CREATE INDEX IF NOT EXISTS idx_lineage_transformations_actual
                    ON lineage_transformations(job_id) WHERE is_actual;
                CREATE INDEX IF NOT EXISTS idx_lineage_sessions_user
                    ON lineage_sessions(user_id);
                """
            )
            self.conn.execute("ALTER TABLE lineage_sync_schedules ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'postgresql'")
            self._ensure_initial_user()
            self._commit()

    def create_user(self, username: str, password: str, role: str) -> dict[str, Any]:
        username = username.strip()
        if not username:
            raise ValueError("Username is required")
        if role not in USER_ROLES:
            raise ValueError("Role must be data_engineer or data_analyst")
        if len(password) < 8:
            raise ValueError("Password must contain at least 8 characters")
        with self._lock:
            row = self.conn.execute(
                """
                INSERT INTO lineage_users(username, role, password_hash)
                VALUES (%s, %s, %s)
                RETURNING id, username, role, created_at
                """,
                (username, role, self._hash_password(password)),
            ).fetchone()
            self._commit()
            return dict(row)

    def authenticate_user(self, username: str, password: str) -> dict[str, Any] | None:
        user = self._one("SELECT * FROM lineage_users WHERE username = %s", (username,))
        if not user or not self._verify_password(password, str(user["password_hash"])):
            return None
        return {"id": user["id"], "username": user["username"], "role": user["role"]}

    def create_session(self, user_id: int) -> dict[str, Any]:
        token = secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + timedelta(days=SESSION_TTL_DAYS)
        with self._lock:
            row = self.conn.execute(
                """
                INSERT INTO lineage_sessions(token, user_id, expires_at)
                VALUES (%s, %s, %s)
                RETURNING token, expires_at
                """,
                (token, user_id, expires_at),
            ).fetchone()
            self._commit()
            return dict(row)

    def user_by_session(self, token: str | None) -> dict[str, Any] | None:
        if not token:
            return None
        user = self._one(
            """
            SELECT lineage_users.id, lineage_users.username, lineage_users.role
            FROM lineage_sessions
            JOIN lineage_users ON lineage_users.id = lineage_sessions.user_id
            WHERE lineage_sessions.token = %s AND lineage_sessions.expires_at > NOW()
            """,
            (token,),
        )
        if user is None:
            self.delete_session(token)
        return user

    def delete_session(self, token: str | None) -> None:
        if not token:
            return
        with self._lock:
            self.conn.execute("DELETE FROM lineage_sessions WHERE token = %s", (token,))
            self._commit()

    def upsert_table(self, table: TableSpec) -> int:
        with self._lock:
            current = self.get_actual_table(table.name)
            if current and (not table.attributes or self._attribute_signature(int(current["id"])) == self._table_signature(table)):
                return int(current["id"])

            version = int(current["version"]) + 1 if current else 1
            if current:
                self._deactivate_table(int(current["id"]))

            table_id = int(
                self.conn.execute(
                    """
                    INSERT INTO lineage_tables(namespace, schema, table_name, version)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                    """,
                    (table.namespace, table.schema, table.table_name, version),
                ).fetchone()["id"]
            )
            for attr in table.attributes:
                self.conn.execute(
                    """
                    INSERT INTO lineage_attributes(table_id, name, data_type, version)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (table_id, attr.name, attr.data_type, version),
                )
            self._commit()
            return table_id

    def table_matches_actual_model(self, table: TableSpec) -> bool:
        current = self.get_actual_table(table.name)
        if current is None:
            return False
        return not table.attributes or self._attribute_signature(int(current["id"])) == self._table_signature(table)

    def mark_missing_tables_inactive_scoped(self, actual_names: Iterable[str], namespace: str, schema: str) -> list[str]:
        with self._lock:
            actual = set(actual_names)
            removed: list[str] = []
            for row in self._rows(
                f"""
                SELECT *, {TABLE_NAME_SQL} AS name
                FROM lineage_tables
                WHERE is_actual = TRUE AND namespace = %s AND schema = %s
                """,
                (namespace, schema),
            ):
                if row["name"] not in actual:
                    self._deactivate_table(int(row["id"]))
                    removed.append(str(row["name"]))
            self._commit()
            return removed

    def upsert_job(self, job: JobSpec, input_table_ids: Iterable[int], output_table_ids: Iterable[int]) -> JobUpsertResult:
        with self._lock:
            input_ids = tuple(input_table_ids)
            output_ids = tuple(output_table_ids)
            current = self.get_actual_job(job.name)
            table_signature = self._job_table_signature_from_ids(input_ids, output_ids)
            if current and current["sql"] == job.sql and current["dialect"] == job.dialect and self._job_table_signature(int(current["id"])) == table_signature:
                return JobUpsertResult(id=int(current["id"]), changed=False)

            version = int(current["version"]) + 1 if current else 1
            if current:
                self._deactivate_job(int(current["id"]))

            job_id = int(
                self.conn.execute(
                    """
                    INSERT INTO lineage_jobs(namespace, name, sql, dialect, version)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (job.namespace, job.name, job.sql, job.dialect, version),
                ).fetchone()["id"]
            )
            self._replace_job_tables(job_id, input_ids, output_ids)
            self._commit()
            return JobUpsertResult(id=job_id, changed=True)

    def replace_transformations(self, job_id: int, transformations: Iterable[TransformationSpec]) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE lineage_transformations SET is_actual = FALSE, valid_to = COALESCE(valid_to, NOW()) WHERE job_id = %s",
                (job_id,),
            )
            for item in transformations:
                output_id = self.find_attribute_id(item.output_table, item.output_attribute)
                if output_id is None:
                    continue
                inputs = item.input_attributes or ((None, None),)
                for input_table, input_attr in inputs:
                    self.conn.execute(
                        """
                        INSERT INTO lineage_transformations(
                            job_id, input_attribute_id, output_attribute_id,
                            lineage_type, lineage_subtype, lineage_description,
                            lineage_masking, lineage_scope, source, expression
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            job_id,
                            self.find_attribute_id(input_table, input_attr) if input_table and input_attr else None,
                            output_id,
                            item.lineage_type,
                            item.lineage_subtype,
                            item.lineage_description,
                            item.lineage_masking,
                            item.lineage_scope,
                            item.source,
                            item.expression,
                        ),
                    )
            self._commit()

    def transformations_match_actual_job(self, job_id: int, transformations: Iterable[TransformationSpec]) -> bool:
        return self._transformation_signature(job_id) == self._transformation_signature_from_specs(transformations)

    def save_event(self, job_id: int | None, event_type: EventType, event_time: str, raw: dict[str, Any]) -> int:
        with self._lock:
            raw_json = json.dumps(raw, ensure_ascii=False)
            existing = self.conn.execute(
                """
                SELECT id FROM lineage_events
                WHERE event_type = %s AND event_time = %s AND raw_json = CAST(%s AS jsonb)
                ORDER BY id LIMIT 1
                """,
                (event_type.value, event_time, raw_json),
            ).fetchone()
            if existing:
                if job_id is not None:
                    self.update_event_job(int(existing["id"]), job_id)
                return int(existing["id"])

            row = self.conn.execute(
                """
                INSERT INTO lineage_events(job_id, event_type, event_time, raw_json)
                VALUES (%s, %s, %s, CAST(%s AS jsonb))
                RETURNING id
                """,
                (job_id, event_type.value, event_time, raw_json),
            ).fetchone()
            self._commit()
            return int(row["id"])

    def update_event_job(self, event_id: int, job_id: int) -> None:
        with self._lock:
            self.conn.execute("UPDATE lineage_events SET job_id = %s WHERE id = %s", (job_id, event_id))
            self._commit()

    def create_sync_schedule(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                """
                INSERT INTO lineage_sync_schedules(name, source_type, warehouse_dsn, namespace, schema_name, cron_expression, enabled)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    payload["name"],
                    payload.get("source_type", "postgresql"),
                    payload["warehouse_dsn"],
                    payload["namespace"],
                    payload["schema_name"],
                    payload.get("cron_expression"),
                    bool(payload.get("enabled", True)),
                ),
            ).fetchone()
            self._commit()
            return dict(row)

    def update_sync_schedule(self, schedule_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock:
            current = self.get_sync_schedule(schedule_id)
            if current is None:
                return None
            merged = {**current, **payload}
            if not payload.get("warehouse_dsn"):
                merged["warehouse_dsn"] = current["warehouse_dsn"]
            row = self.conn.execute(
                """
                UPDATE lineage_sync_schedules
                SET name = %s,
                    source_type = %s,
                    warehouse_dsn = %s,
                    namespace = %s,
                    schema_name = %s,
                    cron_expression = %s,
                    enabled = %s,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING *
                """,
                (
                    merged["name"],
                    merged.get("source_type", "postgresql"),
                    merged["warehouse_dsn"],
                    merged["namespace"],
                    merged["schema_name"],
                    merged.get("cron_expression"),
                    bool(merged.get("enabled", True)),
                    schedule_id,
                ),
            ).fetchone()
            self._commit()
            return dict(row) if row else None

    def delete_sync_schedule(self, schedule_id: int) -> None:
        with self._lock:
            self.conn.execute("DELETE FROM lineage_sync_schedules WHERE id = %s", (schedule_id,))
            self._commit()

    def sync_schedules(self) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM lineage_sync_schedules ORDER BY id")

    def get_sync_schedule(self, schedule_id: int) -> dict[str, Any] | None:
        return self._one("SELECT * FROM lineage_sync_schedules WHERE id = %s", (schedule_id,))

    def record_sync_schedule_run(self, schedule_id: int, result: dict[str, Any] | None, error: str | None) -> None:
        with self._lock:
            if error is None:
                self.conn.execute(
                    """
                    UPDATE lineage_sync_schedules
                    SET last_run_at = NOW(),
                        last_success_at = NOW(),
                        last_error = NULL,
                        last_result = %s::jsonb,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (json.dumps(result or {}, ensure_ascii=False), schedule_id),
                )
            else:
                self.conn.execute(
                    """
                    UPDATE lineage_sync_schedules
                    SET last_run_at = NOW(), last_error = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (error, schedule_id),
                )
            self._commit()

    def get_actual_table(self, name: str) -> dict[str, Any] | None:
        return self._one(
            f"SELECT *, {TABLE_NAME_SQL} AS name FROM lineage_tables WHERE {TABLE_NAME_SQL} = %s AND is_actual = TRUE",
            (name,),
        )

    def table_history(self, name: str) -> list[dict[str, Any]]:
        return self._rows(
            f"""
            SELECT lineage_tables.*, {TABLE_NAME_SQL} AS name, COUNT(lineage_attributes.id) AS attribute_count
            FROM lineage_tables
            LEFT JOIN lineage_attributes ON lineage_attributes.table_id = lineage_tables.id
            WHERE {TABLE_NAME_SQL} = %s
            GROUP BY lineage_tables.id
            ORDER BY lineage_tables.version DESC
            """,
            (name,),
        )

    def get_table_state(self, table_id: int) -> dict[str, Any] | None:
        return self._one(f"SELECT *, {TABLE_NAME_SQL} AS name FROM lineage_tables WHERE id = %s", (table_id,))

    def attributes_by_table_id(self, table_id: int) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM lineage_attributes WHERE table_id = %s ORDER BY name", (table_id,))

    def get_actual_job(self, name: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM lineage_jobs WHERE name = %s AND is_actual = TRUE", (name,))

    def job_history(self, name: str) -> list[dict[str, Any]]:
        return self._rows(
            """
            SELECT lineage_jobs.*, COUNT(lineage_transformations.id) AS transformation_count
            FROM lineage_jobs
            LEFT JOIN lineage_transformations ON lineage_transformations.job_id = lineage_jobs.id
            WHERE lineage_jobs.name = %s
            GROUP BY lineage_jobs.id
            ORDER BY lineage_jobs.version DESC
            """,
            (name,),
        )

    def get_job_state(self, job_id: int) -> dict[str, Any] | None:
        return self._one("SELECT * FROM lineage_jobs WHERE id = %s", (job_id,))

    def transformations_by_job_id(self, job_id: int) -> list[dict[str, Any]]:
        return self._transformation_rows("lineage_transformations.job_id = %s", (job_id,), "lineage_transformations.id")

    def find_attribute_id(self, table_name: str, attribute_name: str) -> int | None:
        row = self.conn.execute(
            f"""
            SELECT lineage_attributes.id
            FROM lineage_attributes
            JOIN lineage_tables ON lineage_tables.id = lineage_attributes.table_id
            WHERE {TABLE_NAME_SQL} = %s
              AND lineage_tables.is_actual = TRUE
              AND lineage_attributes.name = %s
              AND lineage_attributes.is_actual = TRUE
            """,
            (table_name, attribute_name),
        ).fetchone()
        return int(row["id"]) if row else None

    def tables(self) -> list[dict[str, Any]]:
        return self._rows(
            f"""
            SELECT *, {TABLE_NAME_SQL} AS name
            FROM lineage_tables
            WHERE is_actual = TRUE
            ORDER BY namespace, schema, table_name
            """
        )

    def attributes(self) -> list[dict[str, Any]]:
        return self._rows(
            f"""
            SELECT lineage_attributes.*, {TABLE_NAME_SQL} AS table_name
            FROM lineage_attributes
            JOIN lineage_tables ON lineage_tables.id = lineage_attributes.table_id
            WHERE lineage_attributes.is_actual = TRUE AND lineage_tables.is_actual = TRUE
            ORDER BY lineage_tables.namespace, lineage_tables.schema, lineage_tables.table_name, lineage_attributes.name
            """
        )

    def transformations(self) -> list[dict[str, Any]]:
        return self._transformation_rows("lineage_transformations.is_actual = TRUE", (), "lineage_transformations.id")

    def job_usage_counts(self) -> dict[str, int]:
        rows = self.conn.execute(
            f"""
            SELECT {TABLE_NAME_SQL} AS name, COUNT(lineage_events.id) AS count
            FROM lineage_tables
            JOIN lineage_job_tables
                ON lineage_job_tables.table_id = lineage_tables.id
               AND lineage_job_tables.role = %s
            JOIN lineage_events ON lineage_events.job_id = lineage_job_tables.job_id
            WHERE lineage_tables.is_actual = TRUE AND lineage_job_tables.is_actual = TRUE
            GROUP BY lineage_tables.namespace, lineage_tables.schema, lineage_tables.table_name
            """,
            (DatasetRole.INPUT.value,),
        )
        return {str(row["name"]): int(row["count"]) for row in rows}

    def _transformation_rows(self, where: str, params: tuple[object, ...], order_by: str) -> list[dict[str, Any]]:
        return self._rows(
            f"""
            SELECT
                lineage_transformations.*,
                lineage_jobs.name AS job_name,
                lineage_jobs.sql AS job_sql,
                input_tables.namespace || '.' || input_tables.schema || '.' || input_tables.table_name AS input_table,
                input_attrs.name AS input_attribute,
                output_tables.namespace || '.' || output_tables.schema || '.' || output_tables.table_name AS output_table,
                output_attrs.name AS output_attribute
            FROM lineage_transformations
            JOIN lineage_jobs ON lineage_jobs.id = lineage_transformations.job_id
            LEFT JOIN lineage_attributes AS input_attrs ON input_attrs.id = lineage_transformations.input_attribute_id
            LEFT JOIN lineage_tables AS input_tables ON input_tables.id = input_attrs.table_id
            LEFT JOIN lineage_attributes AS output_attrs ON output_attrs.id = lineage_transformations.output_attribute_id
            LEFT JOIN lineage_tables AS output_tables ON output_tables.id = output_attrs.table_id
            WHERE {where}
            ORDER BY {order_by}
            """,
            params,
        )

    def _replace_job_tables(self, job_id: int, input_ids: Iterable[int], output_ids: Iterable[int]) -> None:
        self.conn.execute("DELETE FROM lineage_job_tables WHERE job_id = %s", (job_id,))
        for role, ids in ((DatasetRole.INPUT.value, input_ids), (DatasetRole.OUTPUT.value, output_ids)):
            for table_id in ids:
                self.conn.execute(
                    """
                    INSERT INTO lineage_job_tables(job_id, table_id, role)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (job_id, table_id, role) DO NOTHING
                    """,
                    (job_id, table_id, role),
                )

    def _deactivate_table(self, table_id: int) -> None:
        self.conn.execute("UPDATE lineage_tables SET is_actual = FALSE, valid_to = COALESCE(valid_to, NOW()) WHERE id = %s", (table_id,))
        self.conn.execute("UPDATE lineage_attributes SET is_actual = FALSE, valid_to = COALESCE(valid_to, NOW()) WHERE table_id = %s", (table_id,))

    def _deactivate_job(self, job_id: int) -> None:
        self.conn.execute("UPDATE lineage_jobs SET is_actual = FALSE, valid_to = COALESCE(valid_to, NOW()) WHERE id = %s", (job_id,))
        self.conn.execute("UPDATE lineage_job_tables SET is_actual = FALSE, valid_to = COALESCE(valid_to, NOW()) WHERE job_id = %s", (job_id,))
        self.conn.execute("UPDATE lineage_transformations SET is_actual = FALSE, valid_to = COALESCE(valid_to, NOW()) WHERE job_id = %s", (job_id,))

    def _job_table_signature(self, job_id: int) -> tuple[tuple[str, int], ...]:
        rows = self.conn.execute(
            "SELECT role, table_id FROM lineage_job_tables WHERE job_id = %s AND is_actual = TRUE ORDER BY role, table_id",
            (job_id,),
        )
        return tuple((str(row["role"]), int(row["table_id"])) for row in rows)

    def _job_table_signature_from_ids(self, input_ids: Iterable[int], output_ids: Iterable[int]) -> tuple[tuple[str, int], ...]:
        return tuple(
            sorted(
                [(DatasetRole.INPUT.value, int(table_id)) for table_id in input_ids]
                + [(DatasetRole.OUTPUT.value, int(table_id)) for table_id in output_ids]
            )
        )

    def _attribute_signature(self, table_id: int) -> list[tuple[str, str]]:
        rows = self.conn.execute(
            "SELECT name, data_type FROM lineage_attributes WHERE table_id = %s AND is_actual = TRUE ORDER BY id",
            (table_id,),
        )
        return [(str(row["name"]), str(row["data_type"])) for row in rows]

    def _table_signature(self, table: TableSpec) -> list[tuple[str, str]]:
        return [(attr.name, attr.data_type) for attr in table.attributes]

    def _transformation_signature(self, job_id: int) -> tuple[tuple[object, ...], ...]:
        return tuple(
            sorted(
                (
                    row["input_attribute_id"],
                    row["output_attribute_id"],
                    row["lineage_type"],
                    row["lineage_subtype"],
                    row["lineage_description"],
                    row["lineage_masking"],
                    row["lineage_scope"],
                    row["source"],
                    row["expression"],
                )
                for row in self._rows(
                    """
                    SELECT input_attribute_id, output_attribute_id, lineage_type, lineage_subtype,
                           lineage_description, lineage_masking, lineage_scope, source, expression
                    FROM lineage_transformations
                    WHERE job_id = %s AND is_actual = TRUE
                    """,
                    (job_id,),
                )
            )
        )

    def _transformation_signature_from_specs(self, transformations: Iterable[TransformationSpec]) -> tuple[tuple[object, ...], ...]:
        signature: list[tuple[object, ...]] = []
        for item in transformations:
            output_id = self.find_attribute_id(item.output_table, item.output_attribute)
            if output_id is None:
                continue
            inputs = item.input_attributes or ((None, None),)
            for input_table, input_attr in inputs:
                signature.append(
                    (
                        self.find_attribute_id(input_table, input_attr) if input_table and input_attr else None,
                        output_id,
                        item.lineage_type,
                        item.lineage_subtype,
                        item.lineage_description,
                        item.lineage_masking,
                        item.lineage_scope,
                        item.source,
                        item.expression,
                    )
                )
        return tuple(sorted(signature))

    def _ensure_initial_user(self) -> None:
        if self.conn.execute("SELECT 1 FROM lineage_users LIMIT 1").fetchone():
            return
        username = os.getenv("LINEAGE_ADMIN_USERNAME", "admin")
        password = os.getenv("LINEAGE_ADMIN_PASSWORD")
        if not password:
            password = secrets.token_urlsafe(16)
            print(
                "\nInitial data_engineer user created\n"
                f"Username: {username}\n"
                f"Password: {password}\n",
                flush=True,
            )
        self.conn.execute(
            """
            INSERT INTO lineage_users(username, role, password_hash)
            VALUES (%s, 'data_engineer', %s)
            """,
            (username, self._hash_password(password)),
        )

    def _hash_password(self, password: str) -> str:
        salt = secrets.token_hex(16)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), PASSWORD_ITERATIONS).hex()
        return f"pbkdf2_sha256${PASSWORD_ITERATIONS}${salt}${digest}"

    def _verify_password(self, password: str, stored: str) -> bool:
        try:
            algorithm, iterations, salt, expected = stored.split("$", 3)
        except ValueError:
            return False
        if algorithm != "pbkdf2_sha256":
            return False
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), int(iterations)).hex()
        return hmac.compare_digest(digest, expected)

    def _rows(self, query: str, params: tuple[object, ...] = ()) -> list[dict[str, Any]]:
        return [dict(row) for row in self.conn.execute(query, params)]

    def _one(self, query: str, params: tuple[object, ...] = ()) -> dict[str, Any] | None:
        row = self.conn.execute(query, params).fetchone()
        return dict(row) if row else None

    def _commit(self) -> None:
        if self._transaction_depth == 0:
            self.conn.commit()
