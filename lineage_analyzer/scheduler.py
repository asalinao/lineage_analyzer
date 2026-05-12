from __future__ import annotations

from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .dsn import warehouse_dsn_for_storage
from .repository import MetadataRepository
from .services import LineageService

SOURCE_TYPES = {"postgresql", "greenplum", "clickhouse", "hadoop_spark"}


class SyncScheduler:
    def __init__(self, repository: MetadataRepository, service: LineageService) -> None:
        self.repository = repository
        self.service = service
        self.scheduler = BackgroundScheduler()

    def start(self) -> None:
        for schedule in self.repository.sync_schedules():
            if schedule.get("enabled"):
                self.add_or_update_schedule(schedule)
        self.scheduler.start()

    def shutdown(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)

    def reload(self) -> None:
        self.scheduler.remove_all_jobs()
        for schedule in self.repository.sync_schedules():
            if schedule.get("enabled"):
                self.add_or_update_schedule(schedule)

    def add_or_update_schedule(self, schedule: dict[str, Any]) -> None:
        schedule_id = int(schedule["id"])
        job_id = self._job_id(schedule_id)
        if not schedule.get("enabled"):
            self.remove_schedule(schedule_id)
            return

        trigger = CronTrigger.from_crontab(str(schedule.get("cron_expression") or ""))

        self.scheduler.add_job(
            func=self.service.run_sync_schedule,
            args=[schedule_id],
            trigger=trigger,
            id=job_id,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

    def remove_schedule(self, schedule_id: int) -> None:
        job_id = self._job_id(schedule_id)
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)

    def _job_id(self, schedule_id: int) -> str:
        return f"sync_schedule_{schedule_id}"


def validate_sync_schedule_payload(payload: dict[str, Any], partial: bool = False) -> dict[str, Any]:
    normalized = dict(payload)
    required = ("name", "warehouse_dsn", "namespace", "schema_name", "cron_expression")
    for field in required:
        if not partial and not str(normalized.get(field) or "").strip():
            raise ValueError(f"Field '{field}' is required")

    if "cron_expression" in normalized or not partial:
        cron_expression = str(normalized.get("cron_expression") or "").strip()
        if not cron_expression:
            raise ValueError("cron_expression is required")
        CronTrigger.from_crontab(cron_expression)
        normalized["cron_expression"] = cron_expression

    normalized["enabled"] = bool(normalized.get("enabled", True))
    normalized["source_type"] = str(normalized.get("source_type") or "postgresql").strip().lower().replace("-", "_")
    if normalized["source_type"] not in SOURCE_TYPES:
        raise ValueError(f"source_type must be one of: {', '.join(sorted(SOURCE_TYPES))}")
    for field in ("name", "warehouse_dsn", "namespace", "schema_name"):
        if field in normalized and isinstance(normalized[field], str):
            normalized[field] = normalized[field].strip()
    if "warehouse_dsn" in normalized and normalized["source_type"] == "hadoop_spark":
        normalized["warehouse_dsn"] = str(normalized["warehouse_dsn"]).strip()
    elif "warehouse_dsn" in normalized:
        normalized["warehouse_dsn"] = warehouse_dsn_for_storage(str(normalized["warehouse_dsn"]))
    return normalized
