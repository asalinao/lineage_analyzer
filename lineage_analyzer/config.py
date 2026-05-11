from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote


ENV_FILE = Path.cwd() / ".env"


def load_local_env(path: Path = ENV_FILE) -> None:
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def metadata_dsn(explicit_dsn: str | None = None) -> str | None:
    if explicit_dsn:
        return explicit_dsn
    load_local_env()
    if os.getenv("LINEAGE_DATABASE_URL"):
        return os.environ["LINEAGE_DATABASE_URL"]

    db_name = os.getenv("POSTGRES_DB")
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    if not db_name or not db_user or not db_password:
        return None

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    return (
        f"postgresql://{quote(db_user)}:{quote(db_password)}"
        f"@{host}:{port}/{quote(db_name)}"
    )
