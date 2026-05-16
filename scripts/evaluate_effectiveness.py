from __future__ import annotations

import argparse
import json
import sys
import statistics
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lineage_analyzer.graph import LineageGraph
from lineage_analyzer.models import AttributeChange, AttributeSpec, ChangeType, TableSpec
from lineage_analyzer.sql_parser import SqlParser


def table(name: str, attributes: tuple[str, ...]) -> TableSpec:
    namespace, schema, table_name = name.split(".")
    return TableSpec(
        namespace=namespace,
        schema=schema,
        table_name=table_name,
        attributes=tuple(AttributeSpec(attribute, "text") for attribute in attributes),
    )


SYSTEMS = {
    "postgresql": {"label": "PostgreSQL", "dialect": "postgres", "namespace": "warehouse", "source_schema": "public", "target_schema": "mart"},
    "greenplum": {"label": "Greenplum", "dialect": "postgresql", "namespace": "warehouse", "source_schema": "public", "target_schema": "mart"},
    "clickhouse": {"label": "ClickHouse", "dialect": "clickhouse", "namespace": "clickhouse", "source_schema": "analytics", "target_schema": "mart"},
    "hadoop_spark": {"label": "Hadoop/Hive", "dialect": "hive", "namespace": "hive", "source_schema": "dwh", "target_schema": "mart"},
}


def qname(system: str, schema: str, table_name: str) -> str:
    config = SYSTEMS[system]
    return f"{config['namespace']}.{schema}.{table_name}"


def make_case(system: str, scenario: str) -> dict[str, Any]:
    config = SYSTEMS[system]
    source_schema = str(config["source_schema"])
    target_schema = str(config["target_schema"])
    source_customers = qname(system, source_schema, "customers")
    source_orders = qname(system, source_schema, "orders")

    if scenario == "identity_projection":
        return {
            "name": f"{system}_identity_projection",
            "source_system": system,
            "difficulty": "basic_supported",
            "dialect": config["dialect"],
            "sql": f"select customer_id, email from {source_schema}.customers",
            "inputs": (table(source_customers, ("customer_id", "email")),),
            "outputs": (table(qname(system, target_schema, "dim_customers"), ("customer_id", "email")),),
            "expected": {
                (source_customers, "customer_id", qname(system, target_schema, "dim_customers"), "customer_id"),
                (source_customers, "email", qname(system, target_schema, "dim_customers"), "email"),
            },
        }
    if scenario == "join_and_aggregation":
        return {
            "name": f"{system}_join_and_aggregation",
            "source_system": system,
            "difficulty": "basic_supported",
            "dialect": config["dialect"],
            "sql": f"""
                select c.customer_id, sum(o.amount) as total_amount
                from {source_schema}.customers c
                join {source_schema}.orders o on c.customer_id = o.customer_id
                group by c.customer_id
            """,
            "inputs": (
                table(source_customers, ("customer_id", "email")),
                table(source_orders, ("customer_id", "amount")),
            ),
            "outputs": (table(qname(system, target_schema, "customer_revenue"), ("customer_id", "total_amount", "__dataset__")),),
            "expected": {
                (source_customers, "customer_id", qname(system, target_schema, "customer_revenue"), "customer_id"),
                (source_orders, "amount", qname(system, target_schema, "customer_revenue"), "total_amount"),
                (source_customers, "customer_id", qname(system, target_schema, "customer_revenue"), "__dataset__"),
                (source_orders, "customer_id", qname(system, target_schema, "customer_revenue"), "__dataset__"),
            },
        }
    if scenario == "derived_expression":
        return {
            "name": f"{system}_derived_expression",
            "source_system": system,
            "difficulty": "basic_supported",
            "dialect": config["dialect"],
            "sql": f"select order_id, amount * 1.2 as amount_with_tax from {source_schema}.orders",
            "inputs": (table(source_orders, ("order_id", "amount")),),
            "outputs": (table(qname(system, target_schema, "orders_tax"), ("order_id", "amount_with_tax")),),
            "expected": {
                (source_orders, "order_id", qname(system, target_schema, "orders_tax"), "order_id"),
                (source_orders, "amount", qname(system, target_schema, "orders_tax"), "amount_with_tax"),
            },
        }
    if scenario == "select_star_projection":
        return {
            "name": f"{system}_select_star_projection",
            "source_system": system,
            "difficulty": "hard_sql_construct",
            "dialect": config["dialect"],
            "sql": f"select * from {source_schema}.customers",
            "inputs": (table(source_customers, ("customer_id", "email")),),
            "outputs": (table(qname(system, target_schema, "customers_copy"), ("customer_id", "email")),),
            "expected": {
                (source_customers, "customer_id", qname(system, target_schema, "customers_copy"), "customer_id"),
                (source_customers, "email", qname(system, target_schema, "customers_copy"), "email"),
            },
        }
    if scenario == "ambiguous_unqualified_join_column":
        return {
            "name": f"{system}_ambiguous_unqualified_join_column",
            "source_system": system,
            "difficulty": "hard_sql_construct",
            "dialect": config["dialect"],
            "sql": f"""
                select id
                from {source_schema}.customers c
                join {source_schema}.orders o on c.id = o.id
            """,
            "inputs": (
                table(source_customers, ("id",)),
                table(source_orders, ("id",)),
            ),
            "outputs": (table(qname(system, target_schema, "customer_orders"), ("id", "__dataset__")),),
            "expected": {
                (source_customers, "id", qname(system, target_schema, "customer_orders"), "id"),
                (source_customers, "id", qname(system, target_schema, "customer_orders"), "__dataset__"),
                (source_orders, "id", qname(system, target_schema, "customer_orders"), "__dataset__"),
            },
        }
    if scenario == "cte_source_resolution":
        return {
            "name": f"{system}_cte_source_resolution",
            "source_system": system,
            "difficulty": "hard_sql_construct",
            "dialect": config["dialect"],
            "sql": f"""
                with recent_orders as (
                    select customer_id, amount from {source_schema}.orders where amount > 0
                )
                select r.customer_id, r.amount from recent_orders r
            """,
            "inputs": (table(source_orders, ("customer_id", "amount")),),
            "outputs": (table(qname(system, target_schema, "recent_orders"), ("customer_id", "amount")),),
            "expected": {
                (source_orders, "customer_id", qname(system, target_schema, "recent_orders"), "customer_id"),
                (source_orders, "amount", qname(system, target_schema, "recent_orders"), "amount"),
            },
        }
    raise ValueError(f"Unknown scenario: {scenario}")


SQL_CASES = [
    make_case(system, scenario)
    for scenario in (
        "identity_projection",
        "join_and_aggregation",
        "derived_expression",
        "select_star_projection",
        "ambiguous_unqualified_join_column",
        "cte_source_resolution",
    )
    for system in ("postgresql", "greenplum", "clickhouse", "hadoop_spark")
]


class SyntheticRepository:
    def __init__(self, table_count: int, attributes_per_table: int, fanout: int) -> None:
        self.table_count = table_count
        self.attributes_per_table = attributes_per_table
        self.fanout = fanout
        self._tables = [
            {
                "name": f"warehouse.public.t{i}",
                "namespace": "warehouse",
                "schema": "public",
                "table_name": f"t{i}",
            }
            for i in range(table_count)
        ]
        self._attributes = [
            {
                "table_name": f"warehouse.public.t{i}",
                "name": f"c{j}",
                "data_type": "integer",
            }
            for i in range(table_count)
            for j in range(attributes_per_table)
        ]
        self._transformations = self._build_transformations()

    def tables(self) -> list[dict[str, Any]]:
        return self._tables

    def attributes(self) -> list[dict[str, Any]]:
        return self._attributes

    def transformations(self) -> list[dict[str, Any]]:
        return self._transformations

    def job_usage_counts(self) -> dict[str, int]:
        return {
            f"warehouse.public.t{i}": (self.table_count - i)
            for i in range(self.table_count)
        }

    def _build_transformations(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for source_idx in range(self.table_count):
            for step in range(1, self.fanout + 1):
                target_idx = source_idx + step
                if target_idx >= self.table_count:
                    continue
                for attr_idx in range(self.attributes_per_table):
                    rows.append(
                        {
                            "job_name": f"job_{source_idx}_{target_idx}",
                            "job_sql": None,
                            "input_table": f"warehouse.public.t{source_idx}",
                            "input_attribute": f"c{attr_idx}",
                            "output_table": f"warehouse.public.t{target_idx}",
                            "output_attribute": f"c{attr_idx}",
                            "lineage_type": "SQL",
                            "lineage_subtype": "IDENTITY",
                            "lineage_description": None,
                            "lineage_masking": None,
                            "lineage_scope": "FIELD",
                            "source": "synthetic",
                            "expression": None,
                        }
                    )
        return rows


def evaluate_sql_parser() -> dict[str, Any]:
    parser = SqlParser()
    total_expected = 0
    total_found = 0
    total_true_positive = 0
    cases: list[dict[str, Any]] = []

    for case in SQL_CASES:
        dialect = str(case["dialect"])
        if parser.is_supported(dialect):
            transformations = parser.extract_transformations(
                case["sql"],
                dialect,
                case["inputs"],
                case["outputs"],
            )
            extraction_status = "parsed"
        else:
            transformations = ()
            extraction_status = "unsupported_dialect"
        actual = {
            (input_table, input_attr, item.output_table, item.output_attribute)
            for item in transformations
            for input_table, input_attr in item.input_attributes
        }
        expected = case["expected"]
        true_positive = len(actual & expected)
        total_expected += len(expected)
        total_found += len(actual)
        total_true_positive += true_positive
        cases.append(
            {
                "name": case["name"],
                "source_system": case["source_system"],
                "difficulty": case["difficulty"],
                "dialect": dialect,
                "extraction_status": extraction_status,
                "expected": len(expected),
                "found": len(actual),
                "true_positive": true_positive,
                "false_positive": len(actual - expected),
                "false_negative": len(expected - actual),
                "precision": ratio(true_positive, len(actual)),
                "recall": ratio(true_positive, len(expected)),
            }
        )

    precision = ratio(total_true_positive, total_found)
    recall = ratio(total_true_positive, total_expected)
    return {
        "cases": cases,
        "by_source_system": aggregate_cases(cases, "source_system"),
        "by_difficulty": aggregate_cases(cases, "difficulty"),
        "precision": precision,
        "recall": recall,
        "f1": f1(precision, recall),
    }


def aggregate_cases(cases: list[dict[str, Any]], key: str) -> dict[str, dict[str, float | int]]:
    grouped: dict[str, dict[str, int]] = {}
    for case in cases:
        item = grouped.setdefault(
            str(case[key]),
            {"expected": 0, "found": 0, "true_positive": 0, "false_positive": 0, "false_negative": 0},
        )
        item["expected"] += int(case["expected"])
        item["found"] += int(case["found"])
        item["true_positive"] += int(case["true_positive"])
        item["false_positive"] += int(case["false_positive"])
        item["false_negative"] += int(case["false_negative"])

    result: dict[str, dict[str, float | int]] = {}
    for group, values in sorted(grouped.items()):
        precision = ratio(values["true_positive"], values["found"])
        recall = ratio(values["true_positive"], values["expected"])
        result[group] = {
            **values,
            "precision": precision,
            "recall": recall,
            "f1": f1(precision, recall),
        }
    return result


def evaluate_graph(table_counts: list[int], attributes_per_table: int, fanout: int, repeats: int) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for table_count in table_counts:
        critical_times: list[float] = []
        impact_times: list[float] = []
        top_scores = []
        affected_counts = []
        for _ in range(repeats):
            repository = SyntheticRepository(table_count, attributes_per_table, fanout)
            graph = LineageGraph(repository)  # type: ignore[arg-type]

            started = time.perf_counter()
            critical = graph.critical_nodes()
            critical_times.append(time.perf_counter() - started)
            top_scores.append(asdict(critical[0]) if critical else {})

            started = time.perf_counter()
            paths = graph.impact_paths_from_attributes(
                [
                    AttributeChange(
                        table_name="warehouse.public.t0",
                        attribute_name="c0",
                        change_type=ChangeType.MODIFIED,
                    )
                ]
            )
            impact_times.append(time.perf_counter() - started)
            affected_counts.append(len({(path.affected_table, path.affected_attribute) for path in paths}))

        results.append(
            {
                "tables": table_count,
                "attributes_per_table": attributes_per_table,
                "fanout": fanout,
                "transformations": len(SyntheticRepository(table_count, attributes_per_table, fanout).transformations()),
                "critical_nodes_ms_avg": round(statistics.mean(critical_times) * 1000, 3),
                "impact_analysis_ms_avg": round(statistics.mean(impact_times) * 1000, 3),
                "affected_attribute_points": round(statistics.mean(affected_counts), 2),
                "top_critical_node": top_scores[-1],
            }
        )
    return results


def ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def f1(precision: float, recall: float) -> float:
    return round(2 * precision * recall / (precision + recall), 4) if precision + recall else 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate Lineage Analyzer effectiveness on reproducible synthetic cases.")
    parser.add_argument("--tables", default="25,50,100", help="Comma-separated synthetic graph sizes.")
    parser.add_argument("--attributes", type=int, default=8, help="Attributes per synthetic table.")
    parser.add_argument("--fanout", type=int, default=2, help="Outgoing edges per synthetic table.")
    parser.add_argument("--repeats", type=int, default=5, help="Measurement repeats per graph size.")
    args = parser.parse_args()

    table_counts = [int(value.strip()) for value in args.tables.split(",") if value.strip()]
    result = {
        "sql_lineage_quality": evaluate_sql_parser(),
        "graph_analysis_performance": evaluate_graph(table_counts, args.attributes, args.fanout, args.repeats),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
