from __future__ import annotations

from typing import Iterable

from sqlglot import exp, parse_one

from .models import TableSpec, TransformationSpec


class SqlParser:
    SUPPORTED_DIALECTS = {
        "postgres",
        "postgresql",
        "mysql",
        "sqlite",
        "bigquery",
        "snowflake",
        "hive",
        "clickhouse",
    }

    def is_supported(self, dialect: str | None) -> bool:
        return bool(dialect and dialect.lower() in self.SUPPORTED_DIALECTS)

    def extract_transformations(
        self,
        sql: str,
        dialect: str,
        inputs: Iterable[TableSpec],
        outputs: Iterable[TableSpec],
    ) -> tuple[TransformationSpec, ...]:
        try:
            tree = parse_one(sql, dialect=dialect)
        except Exception:
            print(f"SQL lineage parsing failed: dialect={dialect}", flush=True)
            return ()

        output_table = next(iter(outputs), None)
        if output_table is None:
            return ()

        aliases: dict[str, str] = {}
        input_tables = tuple(inputs)
        for table in tree.find_all(exp.Table):
            table_name_parts = [part for part in (table.catalog, table.db, table.name) if part]
            sql_table_name = ".".join(table_name_parts)
            matched = next(
                (
                    candidate.name
                    for candidate in input_tables
                    if candidate.name == sql_table_name
                    or candidate.name.endswith(f".{sql_table_name}")
                ),
                sql_table_name,
            )
            aliases[table.alias_or_name] = matched
            aliases[table.name] = matched

        transformations: list[TransformationSpec] = []
        for projection in getattr(tree, "expressions", []) or []:
            output_attribute = projection.alias_or_name
            if not output_attribute:
                continue

            columns = list(projection.find_all(exp.Column))
            if not columns:
                continue

            lineage_subtype = "IDENTITY" if isinstance(projection, exp.Column) else "DERIVED"
            if any(isinstance(node, exp.AggFunc) for node in projection.walk()):
                lineage_subtype = "AGGREGATION"

            input_attributes: list[tuple[str, str]] = []
            for column in columns:
                source_table = aliases.get(column.table)
                if source_table is None and len(input_tables) == 1:
                    source_table = input_tables[0].name
                if source_table is None:
                    continue
                input_attributes.append((source_table, column.name))

            if input_attributes:
                transformations.append(
                    TransformationSpec(
                        output_table=output_table.name,
                        output_attribute=output_attribute,
                        input_attributes=tuple(dict.fromkeys(input_attributes)),
                        source="sql_parsing",
                        expression=projection.sql(dialect=dialect),
                        lineage_type="SQL",
                        lineage_subtype=lineage_subtype,
                        lineage_scope="FIELD",
                    )
                )

        for join in tree.find_all(exp.Join):
            for column in join.find_all(exp.Column):
                source_table = aliases.get(column.table)
                if source_table is None:
                    continue
                transformations.append(
                    TransformationSpec(
                        output_table=output_table.name,
                        output_attribute="__dataset__",
                        input_attributes=((source_table, column.name),),
                        source="sql_parsing",
                        expression=join.sql(dialect=dialect),
                        lineage_type="INDIRECT",
                        lineage_subtype="JOIN",
                        lineage_scope="DATASET",
                    )
                )

        return tuple(transformations)
