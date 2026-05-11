from __future__ import annotations

import base64
import json
import re
from collections.abc import Callable
from urllib.parse import parse_qs, unquote, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from .models import AttributeSpec, TableSpec


TYPE_LENGTH_RE = re.compile(r"\s+\(")
Introspector = Callable[[str, str], list[TableSpec]]


def inspect_database(source_type: str, dsn: str, schema: str = "public") -> list[TableSpec]:
    try:
        introspector = {
            "postgresql": inspect_postgresql_database,
            "greenplum": inspect_greenplum_database,
            "clickhouse": inspect_clickhouse_database,
            "hadoop_spark": inspect_hadoop_spark_database,
            "hadoop-spark": inspect_hadoop_spark_database,
        }[source_type.strip().lower()]
    except KeyError as error:
        raise ValueError(f"Unsupported source_type: {source_type}") from error
    return introspector(dsn, schema)


def inspect_postgresql_database(dsn: str, schema: str = "public") -> list[TableSpec]:
    return _inspect_information_schema(dsn, schema)


def inspect_greenplum_database(dsn: str, schema: str = "public") -> list[TableSpec]:
    return _inspect_information_schema(dsn, schema)


def inspect_clickhouse_database(dsn: str, schema: str = "default") -> list[TableSpec]:
    database = schema or "default"
    query = f"""
        SELECT database, table, name, type, position
        FROM system.columns
        WHERE database = '{_clickhouse_quote(database)}'
        ORDER BY database, table, position
        FORMAT JSONEachRow
    """
    rows = _clickhouse_json_each_row(dsn, query)
    grouped: dict[str, list[AttributeSpec]] = {}
    for row in rows:
        key = f"{row['database']}.{row['table']}"
        grouped.setdefault(key, []).append(
            AttributeSpec(name=str(row["name"]), data_type=normalize_data_type(str(row["type"])))
        )
    return [
        TableSpec(
            namespace=database,
            schema=database,
            table_name=table_key.split(".", 1)[1],
            attributes=tuple(attributes),
        )
        for table_key, attributes in grouped.items()
    ]


def inspect_hadoop_spark_database(dsn: str, schema: str = "default") -> list[TableSpec]:
    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Hadoop/Spark introspection requires pyspark. Install dependencies with "
            "`python3 -m pip install -e .` or run the Docker image with Spark support."
        ) from error

    spark = _spark_session(dsn)
    try:
        tables = spark.sql(f"SHOW TABLES IN `{_spark_identifier(schema)}`").collect()
        result: list[TableSpec] = []
        for table in tables:
            if bool(table["isTemporary"]):
                continue
            table_name = str(table["tableName"])
            columns = _spark_columns(
                spark.sql(f"DESCRIBE TABLE `{_spark_identifier(schema)}`.`{_spark_identifier(table_name)}`").collect()
            )
            result.append(
                TableSpec(
                    namespace=schema,
                    schema=schema,
                    table_name=table_name,
                    attributes=tuple(columns),
                )
            )
        return result
    finally:
        spark.stop()


def _spark_session(dsn: str):
    from pyspark.sql import SparkSession

    parts = urlsplit(dsn)
    master = dsn
    options: dict[str, str] = {}
    if parts.scheme == "spark":
        master = urlunsplit(("spark", parts.netloc, parts.path, "", ""))
        options = {key: values[-1] for key, values in parse_qs(parts.query).items() if values}

    builder = SparkSession.builder.appName(options.pop("appName", "lineage-analyzer-introspect"))
    if master:
        builder = builder.master(master)
    for key, value in options.items():
        builder = builder.config(key, value)
    return builder.enableHiveSupport().getOrCreate()


def _spark_columns(rows: list[object]) -> list[AttributeSpec]:
    columns: list[AttributeSpec] = []
    for row in rows:
        name = str(row["col_name"]).strip()  # type: ignore[index]
        if name.startswith("#"):
            break
        if name:
            columns.append(
                AttributeSpec(
                    name=name,
                    data_type=normalize_data_type(str(row["data_type"])),  # type: ignore[index]
                )
            )
    return columns


def _spark_identifier(value: str) -> str:
    return value.replace("`", "``")


def format_postgresql_data_type(row: object) -> str:
    data_type = str(row["data_type"])  # type: ignore[index]
    character_maximum_length = row["character_maximum_length"]  # type: ignore[index]
    numeric_precision = row["numeric_precision"]  # type: ignore[index]
    numeric_scale = row["numeric_scale"]  # type: ignore[index]
    datetime_precision = row["datetime_precision"]  # type: ignore[index]

    if data_type in {"character varying", "character"} and character_maximum_length is not None:
        return normalize_data_type(f"{data_type}({character_maximum_length})")
    if data_type == "numeric" and numeric_precision is not None:
        if numeric_scale is not None:
            return normalize_data_type(f"{data_type}({numeric_precision},{numeric_scale})")
        return normalize_data_type(f"{data_type}({numeric_precision})")
    if data_type in {"time without time zone", "time with time zone", "timestamp without time zone", "timestamp with time zone"}:
        if datetime_precision is not None:
            return normalize_data_type(f"{data_type}({datetime_precision})")
    return normalize_data_type(data_type)


def normalize_data_type(data_type: str) -> str:
    return TYPE_LENGTH_RE.sub("(", data_type.strip())


def _inspect_information_schema(dsn: str, schema: str) -> list[TableSpec]:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "PostgreSQL/Greenplum introspection requires psycopg. Install dependencies with "
            "`python3 -m pip install -e .` or `python3 -m pip install psycopg[binary]`."
        ) from error

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        namespace_row = conn.execute("SELECT current_database() AS namespace").fetchone()
        namespace = str(namespace_row["namespace"])
        rows = conn.execute(
            """
            SELECT
                table_schema,
                table_name,
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                datetime_precision
            FROM information_schema.columns
            WHERE table_schema = %s
            ORDER BY table_schema, table_name, ordinal_position
            """,
            (schema,),
        ).fetchall()

    grouped: dict[str, list[AttributeSpec]] = {}
    for row in rows:
        key = f"{row['table_schema']}.{row['table_name']}"
        grouped.setdefault(key, []).append(
            AttributeSpec(name=str(row["column_name"]), data_type=format_postgresql_data_type(row))
        )
    return [
        TableSpec(
            namespace=namespace,
            schema=table_key.split(".", 1)[0],
            table_name=table_key.split(".", 1)[1],
            attributes=tuple(attributes),
        )
        for table_key, attributes in grouped.items()
    ]


def _clickhouse_json_each_row(dsn: str, query: str) -> list[dict[str, object]]:
    parts = urlsplit(dsn)
    if parts.scheme not in {"http", "https"}:
        raise ValueError("ClickHouse DSN must use HTTP, for example http://user:password@clickhouse:8123/")
    netloc = parts.hostname or ""
    if parts.port:
        netloc += f":{parts.port}"
    url = urlunsplit((parts.scheme, netloc, parts.path or "/", "", ""))
    headers = {"Content-Type": "text/plain; charset=utf-8"}
    if parts.username:
        token = base64.b64encode(f"{unquote(parts.username)}:{unquote(parts.password or '')}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    request = Request(url, data=query.encode("utf-8"), headers=headers, method="POST")
    with urlopen(request, timeout=30) as response:
        content = response.read().decode("utf-8")
    return [json.loads(line) for line in content.splitlines() if line.strip()]


def _clickhouse_quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")
