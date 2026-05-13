from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass

from .models import AttributeChange, ImpactPath
from .repository import MetadataRepository


@dataclass(frozen=True, slots=True)
class CriticalityScore:
    table: str
    influence: float
    connectivity_loss: float
    usage: float
    score: float


class LineageGraph:
    def __init__(self, repository: MetadataRepository) -> None:
        self.repository = repository
        self.table_edges = self._table_edges()

    def downstream_tables(self, table: str) -> list[dict[str, object]]:
        source_attributes = {
            (row["table_name"], row["name"])
            for row in self.repository.attributes()
            if row["table_name"] == table
        }
        attribute_queue = deque((attribute, attribute[1]) for attribute in sorted(source_attributes))
        visited_routes = {(attribute, attribute[1]) for attribute in source_attributes}
        downstream_attributes: dict[tuple[str, str], set[str]] = defaultdict(set)
        transformations_by_input = self._transformations_by_input()

        while attribute_queue:
            current_attribute, parent_attribute = attribute_queue.popleft()
            transformation_queue = deque(transformations_by_input.get(current_attribute, []))

            while transformation_queue:
                transformation = transformation_queue.popleft()
                output_attribute = (
                    str(transformation["output_table"]),
                    str(transformation["output_attribute"]),
                )
                if not transformation.get("output_attribute"):
                    continue
                if output_attribute in source_attributes:
                    continue

                downstream_attributes[output_attribute].add(parent_attribute)
                route = (output_attribute, parent_attribute)
                if route not in visited_routes:
                    visited_routes.add(route)
                    attribute_queue.append((output_attribute, parent_attribute))

        grouped_attributes: dict[str, list[dict[str, object]]] = defaultdict(list)
        for (downstream_table, attribute), parent_attributes in sorted(downstream_attributes.items()):
            grouped_attributes[downstream_table].append(
                {"name": attribute, "parent_attributes": sorted(parent_attributes)}
            )
        return [
            {"table": downstream_table, "attributes": attributes}
            for downstream_table, attributes in sorted(grouped_attributes.items())
        ]

    def dependency_graph(self) -> dict[str, list[dict[str, object]]]:
        attributes_by_table: dict[str, list[dict[str, object]]] = defaultdict(list)
        for attribute in self.repository.attributes():
            attributes_by_table[str(attribute["table_name"])].append(
                {
                    "name": attribute["name"],
                    "data_type": attribute["data_type"],
                }
            )

        nodes = [
            {
                "id": row["name"],
                "label": row["name"],
                "namespace": row["namespace"],
                "schema": row["schema"],
                "table_name": row["table_name"],
                "type": "table",
                "attributes": attributes_by_table[str(row["name"])],
            }
            for row in self.repository.tables()
        ]

        grouped_edges: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
        transformations = self.repository.transformations()
        job_runs = self.repository.job_runs_by_names(str(item["job_name"]) for item in transformations)
        for transformation in transformations:
            if not transformation.get("input_table"):
                continue
            if not transformation.get("output_table"):
                continue
            source = str(transformation["input_table"])
            target = str(transformation["output_table"])
            grouped_edges[(source, target)].append(
                {
                    "job": transformation["job_name"],
                    "job_sql": transformation.get("job_sql"),
                    "input_attribute": transformation["input_attribute"],
                    "output_attribute": transformation["output_attribute"],
                    "lineage_type": transformation.get("lineage_type"),
                    "lineage_subtype": transformation.get("lineage_subtype"),
                    "lineage_description": transformation.get("lineage_description"),
                    "lineage_masking": transformation.get("lineage_masking"),
                    "lineage_scope": transformation.get("lineage_scope"),
                    "source": transformation.get("source"),
                    "expression": transformation.get("expression"),
                }
            )

        edges = [
            {
                "id": f"{source}->{target}",
                "source": source,
                "target": target,
                "jobs": sorted({str(item["job"]) for item in transformations}),
                "job_runs": [
                    run
                    for job_name in sorted({str(item["job"]) for item in transformations})
                    for run in job_runs.get(job_name, [])
                ],
                "job_sql": [
                    {"job": job_name, "sql": sql}
                    for job_name, sql in sorted(
                        {
                            (str(item["job"]), str(item.get("job_sql") or ""))
                            for item in transformations
                            if item.get("job_sql")
                        }
                    )
                ],
                "attributes": sorted(
                    transformations,
                    key=lambda item: (
                        str(item["job"]),
                        str(item["output_attribute"]),
                        str(item["input_attribute"]),
                    ),
                ),
            }
            for (source, target), transformations in sorted(grouped_edges.items())
        ]
        return {"nodes": nodes, "edges": edges}

    def critical_nodes(
        self,
        alpha: float = 0.5,
        beta: float = 0.3,
        gamma: float = 0.2,
    ) -> list[CriticalityScore]:
        tables = [row["name"] for row in self.repository.tables()]
        usage_counts = self.repository.job_usage_counts()
        influence = {table: self._influence(table) for table in tables}
        connectivity = {table: self._connectivity_loss(table, tables) for table in tables}
        usage = {table: float(usage_counts.get(table, 0)) for table in tables}

        normalized_influence = _normalize(influence)
        normalized_connectivity = _normalize(connectivity)
        normalized_usage = _normalize(usage)

        scores = [
            CriticalityScore(
                table=table,
                influence=influence[table],
                connectivity_loss=connectivity[table],
                usage=usage[table],
                score=(
                    alpha * normalized_influence[table]
                    + beta * normalized_connectivity[table]
                    + gamma * normalized_usage[table]
                ),
            )
            for table in tables
        ]
        return sorted(scores, key=lambda item: item.score, reverse=True)

    def impact_paths_from_attributes(self, changes: list[AttributeChange]) -> list[ImpactPath]:
        transformations_by_input = self._transformations_by_input()
        queue = deque(
            (
                change.table_name,
                change.attribute_name,
                change.table_name,
                change.attribute_name,
                0,
            )
            for change in changes
        )
        visited_routes: set[tuple[str, str, str, str, str]] = set()
        paths: list[ImpactPath] = []

        while queue:
            source_table, source_attribute, current_table, current_attribute, depth = queue.popleft()
            for transformation in transformations_by_input.get((current_table, current_attribute), []):
                output_table = transformation.get("output_table")
                output_attribute = transformation.get("output_attribute")
                job_name = str(transformation.get("job_name") or "")
                if not output_table or not output_attribute:
                    continue
                route_key = (
                    current_table,
                    current_attribute,
                    str(output_table),
                    str(output_attribute),
                    job_name,
                )
                if route_key in visited_routes:
                    continue
                visited_routes.add(route_key)

                impact_path = ImpactPath(
                    source_table=source_table,
                    source_attribute=source_attribute,
                    affected_table=str(output_table),
                    affected_attribute=str(output_attribute),
                    depth=depth + 1,
                )
                paths.append(impact_path)
                queue.append(
                    (
                        source_table,
                        source_attribute,
                        str(output_table),
                        str(output_attribute),
                        depth + 1,
                    )
                )

        return paths

    def _table_edges(self) -> dict[str, dict[str, float]]:
        grouped: dict[tuple[str, str], set[str]] = defaultdict(set)
        output_attribute_inputs: dict[tuple[str, str], set[str]] = defaultdict(set)
        for row in self.repository.transformations():
            output_key = (row["output_table"], row["output_attribute"])
            if not row.get("output_attribute"):
                continue
            if row.get("input_table"):
                output_attribute_inputs[output_key].add(row["input_table"])

        for row in self.repository.transformations():
            if not row.get("input_table"):
                continue
            if not row.get("output_attribute"):
                continue
            grouped[(row["input_table"], row["output_table"])].add(row["output_attribute"])

        output_counts: dict[str, int] = defaultdict(int)
        for row in self.repository.attributes():
            output_counts[row["table_name"]] += 1

        edges: dict[str, dict[str, float]] = defaultdict(dict)
        for (source, target), attributes in grouped.items():
            weighted = 0.0
            for attribute in attributes:
                source_count = max(1, len(output_attribute_inputs[(target, attribute)]))
                weighted += 1 / source_count
            edges[source][target] = weighted / max(1, output_counts[target])
        return edges

    def _transformations_by_input(self) -> dict[tuple[str, str], list[dict[str, object]]]:
        transformations_by_input: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
        for row in self.repository.transformations():
            if row.get("input_table") and row.get("input_attribute"):
                transformations_by_input[(row["input_table"], row["input_attribute"])].append(row)
        return transformations_by_input

    def _influence(self, source: str) -> float:
        total = 0.0
        queue = deque([(source, 1.0)])
        best_seen: dict[str, float] = {}
        while queue:
            current, path_weight = queue.popleft()
            for target, edge_weight in self.table_edges.get(current, {}).items():
                influence = path_weight * edge_weight
                if influence <= best_seen.get(target, 0.0):
                    continue
                best_seen[target] = influence
                queue.append((target, influence))
        total = sum(best_seen.values())
        return total

    def _connectivity_loss(self, removed: str, tables: list[str]) -> float:
        original = _reachable_pairs(self.table_edges, tables)
        if original == 0:
            return 0.0
        reduced_edges = {source: dict(targets) for source, targets in self.table_edges.items()}
        reduced_edges[removed] = {}
        return (original - _reachable_pairs(reduced_edges, tables)) / original


def _reachable_pairs(edges: dict[str, dict[str, float]], tables: list[str]) -> int:
    count = 0
    for source in tables:
        visited: set[str] = set()
        queue = deque([source])
        while queue:
            current = queue.popleft()
            for target in edges.get(current, {}):
                if target in visited:
                    continue
                visited.add(target)
                queue.append(target)
        visited.discard(source)
        count += len(visited)
    return count


def _normalize(values: dict[str, float]) -> dict[str, float]:
    if not values:
        return {}
    maximum = max(values.values())
    if maximum == 0:
        return {key: 0.0 for key in values}
    return {key: value / maximum for key, value in values.items()}
