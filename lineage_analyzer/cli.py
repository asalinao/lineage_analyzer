from __future__ import annotations

import argparse
import getpass
import json
from dataclasses import asdict
from pathlib import Path

from .api import run_server
from .config import load_local_env, metadata_dsn
from .db_introspect import inspect_database
from .graph import LineageGraph
from .logging_config import configure_logging
from .repository import MetadataRepository
from .services import LineageService


def main() -> None:
    load_local_env()
    configure_logging()
    parser = argparse.ArgumentParser(prog="lineage-analyzer")
    parser.add_argument(
        "--dsn",
        default=None,
        help="PostgreSQL DSN for metadata storage. Defaults to .env or LINEAGE_DATABASE_URL.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser("ingest-openlineage", help="Load an OpenLineage JSON event")
    ingest.add_argument("event", type=Path)

    for command, source_type, help_text in (
        ("sync-postgres", "postgresql", "Read metadata from a PostgreSQL warehouse"),
        ("sync-greenplum", "greenplum", "Read metadata from a Greenplum warehouse"),
        ("sync-clickhouse", "clickhouse", "Read metadata from a ClickHouse database"),
        ("sync-hadoop-spark", "hadoop_spark", "Read Hadoop/Hive table metadata through Spark"),
    ):
        sync = subparsers.add_parser(command, help=help_text)
        sync.add_argument("warehouse_dsn")
        sync.add_argument("--schema", default="public")
        sync.set_defaults(source_type=source_type)

    downstream = subparsers.add_parser("downstream", help="Find downstream tables")
    downstream.add_argument("table")

    critical = subparsers.add_parser("critical", help="Calculate critical table scores")
    critical.add_argument("--limit", type=int, default=10)

    impact_table = subparsers.add_parser("impact-table", help="Compare two table versions and build impact report")
    impact_table.add_argument("old_table_id", type=int)
    impact_table.add_argument("new_table_id", type=int)

    impact_job = subparsers.add_parser("impact-job", help="Compare two job versions and build impact report")
    impact_job.add_argument("old_job_id", type=int)
    impact_job.add_argument("new_job_id", type=int)

    serve = subparsers.add_parser("serve", help="Start the web UI and JSON API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)

    create_user = subparsers.add_parser("create-user", help="Create a UI user")
    create_user.add_argument("username")
    create_user.add_argument("--role", choices=("data_engineer", "data_analyst"), required=True)
    create_user.add_argument("--password", default=None)

    args = parser.parse_args()
    dsn = require_dsn(args.dsn)
    if args.command == "serve":
        run_server(dsn, args.host, args.port)
        return

    repository = MetadataRepository(dsn)
    service = LineageService(repository)
    try:
        if args.command == "ingest-openlineage":
            payload = json.loads(args.event.read_text(encoding="utf-8"))
            print_json(service.ingest_event(payload))
        elif args.command in {"sync-postgres", "sync-greenplum", "sync-clickhouse", "sync-hadoop-spark"}:
            tables = inspect_database(
                args.source_type,
                args.warehouse_dsn,
                schema=args.schema,
            )
            print_json(service.sync_database_metadata(tables, schema=args.schema))
        elif args.command == "downstream":
            print_json(LineageGraph(repository).downstream_tables(args.table))
        elif args.command == "critical":
            scores = LineageGraph(repository).critical_nodes()[: args.limit]
            print_json([asdict(score) for score in scores])
        elif args.command == "impact-table":
            print_json(service.compare_table_states(args.old_table_id, args.new_table_id))
        elif args.command == "impact-job":
            print_json(service.compare_job_states(args.old_job_id, args.new_job_id))
        elif args.command == "create-user":
            password = args.password or getpass.getpass("Password: ")
            if not args.password:
                repeated = getpass.getpass("Repeat password: ")
                if password != repeated:
                    raise SystemExit("Passwords do not match")
            print_json(repository.create_user(args.username, password, args.role))
    finally:
        repository.close()


def print_json(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def require_dsn(value: str | None) -> str:
    dsn = metadata_dsn(value)
    if dsn:
        return dsn
    raise SystemExit(
        "PostgreSQL DSN is required. Pass --dsn, set LINEAGE_DATABASE_URL, "
        "or create a local .env file from .env.example."
    )


if __name__ == "__main__":
    main()
