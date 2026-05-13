from __future__ import annotations

import json
import mimetypes
from http.cookies import SimpleCookie
from dataclasses import asdict
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .graph import LineageGraph
from .repository import MetadataRepository
from .scheduler import SyncScheduler, validate_sync_schedule_payload
from .services import LineageService


UI_DIR = Path(__file__).with_name("ui")
SESSION_COOKIE = "lineage_session"


class LineageWebServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], dsn: str) -> None:
        super().__init__(server_address, LineageRequestHandler)
        self.repository = MetadataRepository(dsn)
        self.service = LineageService(self.repository)
        self.scheduler = SyncScheduler(self.repository, self.service)
        self.scheduler.start()

    def server_close(self) -> None:
        self.scheduler.shutdown()
        self.repository.close()
        super().server_close()


class LineageRequestHandler(BaseHTTPRequestHandler):
    server: LineageWebServer

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path == "/":
                self._send_file(UI_DIR / "index.html")
            elif path.startswith("/ui/"):
                self._send_ui_asset(path.removeprefix("/ui/"))
            elif path == "/health":
                self._send_json({"status": "ok"})
            elif path == "/auth/me":
                user = self._current_user()
                self._send_json({"authenticated": bool(user), "user": self._public_user(user) if user else None})
            elif path == "/tables":
                if not self._require_auth():
                    return
                self._send_json(self.server.repository.tables())
            elif path == "/graph":
                if not self._require_auth():
                    return
                self._send_json(LineageGraph(self.server.repository).dependency_graph())
            elif path == "/graph/downstream":
                if not self._require_auth():
                    return
                table = self._required_query_param(parsed.query, "table")
                downstream = LineageGraph(self.server.repository).downstream_tables(table)
                self._send_json({"table": table, "downstream": downstream})
            elif path == "/analysis/critical":
                if not self._require_role("data_engineer"):
                    return
                scores = LineageGraph(self.server.repository).critical_nodes()
                self._send_json([asdict(score) for score in scores])
            elif path == "/history/table":
                if not self._require_auth():
                    return
                name = self._required_query_param(parsed.query, "name")
                self._send_json(self.server.repository.table_history(name))
            elif path == "/history/job":
                if not self._require_auth():
                    return
                name = self._required_query_param(parsed.query, "name")
                self._send_json(self.server.repository.job_history(name))
            elif path == "/analysis/impact/table":
                if not self._require_auth():
                    return
                old_table_id = int(self._required_query_param(parsed.query, "old_table_id"))
                new_table_id = int(self._required_query_param(parsed.query, "new_table_id"))
                self._send_json(self.server.service.compare_table_states(old_table_id, new_table_id))
            elif path == "/analysis/impact/job":
                if not self._require_auth():
                    return
                old_job_id = int(self._required_query_param(parsed.query, "old_job_id"))
                new_job_id = int(self._required_query_param(parsed.query, "new_job_id"))
                self._send_json(self.server.service.compare_job_states(old_job_id, new_job_id))
            elif path == "/sync-schedules":
                if not self._require_role("data_engineer"):
                    return
                self._send_json([self._public_schedule(row) for row in self.server.repository.sync_schedules()])
            else:
                self._send_error(HTTPStatus.NOT_FOUND, "Route not found")
        except ValueError as error:
            self._send_error(HTTPStatus.BAD_REQUEST, str(error))
        except LookupError as error:
            self._send_error(HTTPStatus.NOT_FOUND, str(error))
        except Exception as error:  # pragma: no cover - defensive HTTP boundary
            self.server.repository.rollback()
            self._send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(error))

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            parts = self._path_parts(parsed.path)
            if parsed.path == "/auth/login":
                payload = self._read_json_body()
                user = self.server.repository.authenticate_user(str(payload.get("username") or ""), str(payload.get("password") or ""))
                if user is None:
                    self._send_error(HTTPStatus.UNAUTHORIZED, "Invalid username or password")
                    return
                session = self.server.repository.create_session(int(user["id"]))
                self._send_json({"user": self._public_user(user)}, headers=[self._session_cookie(str(session["token"]))])
            elif parsed.path == "/auth/logout":
                self.server.repository.delete_session(self._session_token())
                self._send_json({"logged_out": True}, headers=[self._clear_session_cookie()])
            elif parsed.path == "/openlineage/events":
                payload = self._read_json_body()
                result = self.server.service.process_openlineage_event(payload)
                self._send_json(result, status=HTTPStatus.ACCEPTED)
            elif parts == ["sync-schedules"]:
                if not self._require_role("data_engineer"):
                    return
                payload = validate_sync_schedule_payload(self._read_json_body())
                schedule = self.server.repository.create_sync_schedule(payload)
                self.server.scheduler.add_or_update_schedule(schedule)
                self._send_json(self._public_schedule(schedule), status=HTTPStatus.CREATED)
            elif len(parts) == 3 and parts[0] == "sync-schedules" and parts[2] == "run":
                if not self._require_role("data_engineer"):
                    return
                if self.server.repository.get_sync_schedule(int(parts[1])) is None:
                    self._send_error(HTTPStatus.NOT_FOUND, "Sync schedule not found")
                    return
                result = self.server.service.run_sync_schedule(int(parts[1]))
                self._send_json(result)
            else:
                self._send_error(HTTPStatus.NOT_FOUND, "Route not found")
        except json.JSONDecodeError:
            self._send_error(HTTPStatus.BAD_REQUEST, "Request body must be valid JSON")
        except ValueError as error:
            self._send_error(HTTPStatus.BAD_REQUEST, str(error))
        except Exception as error:  # pragma: no cover - defensive HTTP boundary
            self.server.repository.rollback()
            self._send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(error))

    def do_PUT(self) -> None:
        parsed = urlparse(self.path)
        try:
            parts = self._path_parts(parsed.path)
            if len(parts) != 2 or parts[0] != "sync-schedules":
                self._send_error(HTTPStatus.NOT_FOUND, "Route not found")
                return
            if not self._require_role("data_engineer"):
                return
            schedule_id = int(parts[1])
            payload = validate_sync_schedule_payload(self._read_json_body(), partial=True)
            schedule = self.server.repository.update_sync_schedule(schedule_id, payload)
            if schedule is None:
                self._send_error(HTTPStatus.NOT_FOUND, "Sync schedule not found")
                return
            self.server.scheduler.add_or_update_schedule(schedule)
            self._send_json(self._public_schedule(schedule))
        except json.JSONDecodeError:
            self._send_error(HTTPStatus.BAD_REQUEST, "Request body must be valid JSON")
        except ValueError as error:
            self._send_error(HTTPStatus.BAD_REQUEST, str(error))
        except Exception as error:  # pragma: no cover - defensive HTTP boundary
            self.server.repository.rollback()
            self._send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(error))

    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        try:
            parts = self._path_parts(parsed.path)
            if len(parts) != 2 or parts[0] != "sync-schedules":
                self._send_error(HTTPStatus.NOT_FOUND, "Route not found")
                return
            if not self._require_role("data_engineer"):
                return
            schedule_id = int(parts[1])
            self.server.scheduler.remove_schedule(schedule_id)
            self.server.repository.delete_sync_schedule(schedule_id)
            self._send_json({"deleted": True})
        except ValueError as error:
            self._send_error(HTTPStatus.BAD_REQUEST, str(error))
        except Exception as error:  # pragma: no cover - defensive HTTP boundary
            self.server.repository.rollback()
            self._send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(error))

    def log_message(self, format: str, *args: object) -> None:
        return

    def _required_query_param(self, query: str, name: str) -> str:
        values = parse_qs(query).get(name)
        if not values or not values[0]:
            raise ValueError(f"Query parameter '{name}' is required")
        return values[0]

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        payload = json.loads(body.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Request body must be a JSON object")
        return payload

    def _path_parts(self, path: str) -> list[str]:
        return [part for part in path.split("/") if part]

    def _public_schedule(self, schedule: dict[str, Any]) -> dict[str, Any]:
        public = dict(schedule)
        public["warehouse_dsn"] = mask_dsn(str(public.get("warehouse_dsn") or ""))
        return public

    def _current_user(self) -> dict[str, Any] | None:
        return self.server.repository.user_by_session(self._session_token())

    def _public_user(self, user: dict[str, Any] | None) -> dict[str, Any] | None:
        if user is None:
            return None
        return {"id": user["id"], "username": user["username"], "role": user["role"]}

    def _require_auth(self) -> dict[str, Any] | None:
        user = self._current_user()
        if user is None:
            self._send_error(HTTPStatus.UNAUTHORIZED, "Authentication required")
            return None
        return user

    def _require_role(self, role: str) -> dict[str, Any] | None:
        user = self._require_auth()
        if user is None:
            return None
        if user["role"] != role:
            self._send_error(HTTPStatus.FORBIDDEN, "Access denied")
            return None
        return user

    def _session_token(self) -> str | None:
        cookie = SimpleCookie(self.headers.get("Cookie", ""))
        morsel = cookie.get(SESSION_COOKIE)
        return morsel.value if morsel else None

    def _session_cookie(self, token: str) -> tuple[str, str]:
        cookie = SimpleCookie()
        cookie[SESSION_COOKIE] = token
        cookie[SESSION_COOKIE]["path"] = "/"
        cookie[SESSION_COOKIE]["httponly"] = True
        cookie[SESSION_COOKIE]["samesite"] = "Lax"
        cookie[SESSION_COOKIE]["max-age"] = str(7 * 24 * 60 * 60)
        return ("Set-Cookie", cookie.output(header="").strip())

    def _clear_session_cookie(self) -> tuple[str, str]:
        return ("Set-Cookie", f"{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0")

    def _send_ui_asset(self, asset: str) -> None:
        relative = Path(unquote(asset))
        if relative.is_absolute() or ".." in relative.parts:
            self._send_error(HTTPStatus.BAD_REQUEST, "Invalid asset path")
            return
        self._send_file(UI_DIR / relative)

    def _send_file(self, path: Path) -> None:
        if not path.is_file():
            self._send_error(HTTPStatus.NOT_FOUND, "File not found")
            return
        content = path.read_bytes()
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        if path.parent == UI_DIR:
            self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content)

    def _send_json(self, payload: object, status: HTTPStatus = HTTPStatus.OK, headers: list[tuple[str, str]] | None = None) -> None:
        content = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        for name, value in headers or []:
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(content)

    def _send_error(self, status: HTTPStatus, message: str) -> None:
        self._send_json({"error": message}, status=status)


def run_server(dsn: str, host: str = "127.0.0.1", port: int = 8080) -> None:
    address = (host, port)
    with LineageWebServer(address, dsn) as server:
        print(f"Lineage Analyzer UI: http://{host}:{port}/")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped")


def mask_dsn(dsn: str) -> str:
    if "://" not in dsn or "@" not in dsn:
        return dsn
    prefix, rest = dsn.split("://", 1)
    credentials, host = rest.split("@", 1)
    if ":" not in credentials:
        return f"{prefix}://***@{host}"
    user, _password = credentials.split(":", 1)
    return f"{prefix}://{user}:***@{host}"
