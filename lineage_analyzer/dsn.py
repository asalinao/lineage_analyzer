from __future__ import annotations

import json
import os
import re
from hashlib import sha256
from pathlib import Path
from urllib.parse import SplitResult, quote, quote_plus, unquote, urlsplit, urlunsplit


ENV_PASSWORD_RE = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")
WAREHOUSE_ENV_FILE = "LINEAGE_WAREHOUSE_ENV_FILE"


def normalize_warehouse_dsn(dsn: str) -> str:
    value = dsn.strip()
    if not value:
        return value

    parts = urlsplit(value)
    if parts.scheme and parts.netloc:
        return urlunsplit((parts.scheme, parts.netloc, "/", "", ""))
    return value


def database_dsn(warehouse_dsn: str, namespace: str) -> str:
    return resolve_warehouse_dsn_password(normalize_warehouse_dsn(warehouse_dsn)).rstrip("/") + "/" + quote(namespace.strip())


def warehouse_dsn_for_storage(dsn: str) -> str:
    load_warehouse_password_env()
    normalized = normalize_warehouse_dsn(dsn)
    parts = urlsplit(normalized)
    if not parts.password:
        return normalized

    password = unquote(parts.password)
    if ENV_PASSWORD_RE.match(password):
        return normalized

    env_name = "LINEAGE_WAREHOUSE_PASSWORD_" + sha256(_dsn_without_password(parts).encode()).hexdigest()[:16].upper()
    os.environ[env_name] = password
    persist_warehouse_password(env_name, password)
    return _dsn_with_password(parts, f"${{{env_name}}}")


def validate_warehouse_dsn_secrets(dsn: str) -> None:
    password = urlsplit(dsn).password
    if password and not ENV_PASSWORD_RE.match(unquote(password)):
        raise ValueError("warehouse_dsn password must be an environment variable reference like ${WAREHOUSE_PASSWORD}")


def resolve_warehouse_dsn_password(dsn: str) -> str:
    load_warehouse_password_env()
    parts = urlsplit(dsn)
    if not parts.password:
        return dsn

    match = ENV_PASSWORD_RE.match(unquote(parts.password))
    if not match:
        raise ValueError("warehouse_dsn password must be an environment variable reference like ${WAREHOUSE_PASSWORD}")

    env_name = match.group(1)
    password = os.getenv(env_name)
    if password is None:
        raise ValueError(f"Environment variable '{env_name}' is required for warehouse_dsn password")

    username = quote_plus(unquote(parts.username or ""))
    password = quote_plus(password)
    host = parts.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = f":{parts.port}" if parts.port else ""
    return urlunsplit((parts.scheme, f"{username}:{password}@{host}{port}", parts.path, parts.query, parts.fragment))


def load_warehouse_password_env() -> None:
    path = _warehouse_env_path()
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = _env_unquote(value.strip())
        if key:
            os.environ.setdefault(key, value)


def persist_warehouse_password(name: str, value: str) -> None:
    path = _warehouse_env_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    values: dict[str, str] = {}
    if path.is_file():
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, current = line.split("=", 1)
                values[key.strip()] = _env_unquote(current.strip())
    values[name] = value
    content = "\n".join(f"{key}={_env_quote(values[key])}" for key in sorted(values)) + "\n"
    path.write_text(content, encoding="utf-8")
    path.chmod(0o600)


def _dsn_without_password(parts: SplitResult) -> str:
    return _dsn_with_password(parts, "")


def _dsn_with_password(parts: SplitResult, password: str) -> str:
    username = quote_plus(unquote(parts.username or ""))
    host = parts.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = f":{parts.port}" if parts.port else ""
    credentials = f"{username}:{quote_plus(password)}" if password else username
    return urlunsplit((parts.scheme, f"{credentials}@{host}{port}", parts.path, parts.query, parts.fragment))


def _warehouse_env_path() -> Path:
    return Path(os.getenv(WAREHOUSE_ENV_FILE, ".lineage_warehouse.env"))


def _env_quote(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _env_unquote(value: str) -> str:
    if value.startswith('"') and value.endswith('"'):
        return str(json.loads(value))
    return value.strip("'")
