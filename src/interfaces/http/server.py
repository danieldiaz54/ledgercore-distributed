"""Zero-dependency HTTP/1.1 REST Server built on Python 3.12 standard library.

Exposes the LedgerCore Distributed Saga, Double-Entry Accounting, Transactional
Outbox Relay, Kafka Topic Inspector, and Dead-Letter Queue API v1 on port 8096.
"""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from src.application.services import LedgerCoreApplicationService
from src.domain.exceptions import (
    AccountNotFoundError,
    DistributedLockContentionError,
    DomainError,
    IdempotencyConflictError,
    InsufficientFundsError,
    InvalidTransferError,
    ZeroSumInvariantViolationError,
)

MAX_JSON_BODY_BYTES = 64 * 1024  # 64 KB strict payload limit
MAX_QUERY_LIMIT = 500
REPO_ROOT = Path(__file__).resolve().parents[3]
UI_INDEX_PATH = REPO_ROOT / "ui" / "index.html"


class PayloadTooLargeError(ValueError):
    """Raised when an HTTP request body exceeds MAX_JSON_BODY_BYTES (64 KB)."""


class MalformedJsonError(ValueError):
    """Raised when an HTTP request body is missing, invalid UTF-8, or not a JSON object."""


def map_exception_to_http(exc: Exception) -> tuple[int, dict[str, Any]]:
    """Map typed domain and transport exceptions to semantic HTTP codes without stack traces."""
    if isinstance(exc, PayloadTooLargeError):
        return (
            413,
            {
                "error": {
                    "code": "ERR-413-PAYLOAD-TOO-LARGE",
                    "type": "PayloadTooLargeError",
                    "message": str(exc),
                    "details": {"max_bytes": MAX_JSON_BODY_BYTES},
                }
            },
        )

    if isinstance(exc, MalformedJsonError):
        return (
            400,
            {
                "error": {
                    "code": "ERR-400-MALFORMED-JSON",
                    "type": "MalformedJsonError",
                    "message": str(exc),
                    "details": {},
                }
            },
        )

    if isinstance(exc, InvalidTransferError):
        return (
            400,
            {
                "error": {
                    "code": exc.code,
                    "type": exc.__class__.__name__,
                    "message": exc.message,
                    "details": exc.details,
                }
            },
        )

    if isinstance(exc, AccountNotFoundError):
        return (
            404,
            {
                "error": {
                    "code": exc.code,
                    "type": exc.__class__.__name__,
                    "message": exc.message,
                    "details": exc.details,
                }
            },
        )

    if isinstance(exc, (IdempotencyConflictError, DistributedLockContentionError)):
        return (
            409,
            {
                "error": {
                    "code": exc.code,
                    "type": exc.__class__.__name__,
                    "message": exc.message,
                    "details": exc.details,
                }
            },
        )

    if isinstance(exc, InsufficientFundsError):
        return (
            422,
            {
                "error": {
                    "code": exc.code,
                    "type": exc.__class__.__name__,
                    "message": exc.message,
                    "details": exc.details,
                }
            },
        )

    if isinstance(exc, ZeroSumInvariantViolationError):
        return (
            500,
            {
                "error": {
                    "code": exc.code,
                    "type": exc.__class__.__name__,
                    "message": exc.message,
                    "details": exc.details,
                }
            },
        )

    if isinstance(exc, DomainError):
        return (
            400,
            {
                "error": {
                    "code": exc.code,
                    "type": exc.__class__.__name__,
                    "message": exc.message,
                    "details": exc.details,
                }
            },
        )

    return (
        500,
        {
            "error": {
                "code": "ERR-500-INTERNAL-ERROR",
                "type": "InternalServerError",
                "message": "An unexpected internal server error occurred.",
                "details": {},
            }
        },
    )


def _parse_positive_limit(qs: dict[str, list[str]], default: int) -> int:
    raw_values = qs.get("limit")
    if not raw_values:
        return default
    raw = raw_values[0].strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise InvalidTransferError(
            "Query parameter 'limit' must be a positive integer.",
            details={"field": "limit", "value": raw},
        ) from exc
    if parsed < 1 or parsed > MAX_QUERY_LIMIT:
        raise InvalidTransferError(
            f"Query parameter 'limit' must be between 1 and {MAX_QUERY_LIMIT}.",
            details={"field": "limit", "value": parsed, "max": MAX_QUERY_LIMIT},
        )
    return parsed


class LedgerCoreThreadingHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer bound to a ``LedgerCoreApplicationService`` instance."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        RequestHandlerClass: type[BaseHTTPRequestHandler],
        service: LedgerCoreApplicationService,
        *,
        owns_service: bool = False,
    ) -> None:
        self.service = service
        self._owns_service = owns_service
        super().__init__(server_address, RequestHandlerClass)

    def server_close(self) -> None:
        super().server_close()
        if self._owns_service:
            self.service.close()


class LedgerCoreHTTPRequestHandler(BaseHTTPRequestHandler):
    """HTTP/1.1 REST request handler for LedgerCore Distributed API v1."""

    protocol_version = "HTTP/1.1"
    server_version = "LedgerCore/1.0"
    sys_version = ""

    @property
    def service(self) -> LedgerCoreApplicationService:
        server = self.server
        if isinstance(server, LedgerCoreThreadingHTTPServer):
            return server.service
        raise RuntimeError("Server is not configured with LedgerCoreApplicationService.")

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress noisy stderr logs during test runs unless explicitly enabled."""
        if os.environ.get("LEDGERCORE_ACCESS_LOG", "0") == "1":
            super().log_message(format, *args)

    def _send_security_and_cors_headers(self) -> None:
        cors_origin = os.environ.get("LEDGERCORE_CORS_ORIGIN", "*")
        self.send_header("Access-Control-Allow-Origin", cors_origin)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header(
            "Access-Control-Allow-Headers",
            "Content-Type, Accept, Idempotency-Key, X-Request-ID",
        )
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")

    def _send_json(self, status_code: int, payload: dict[str, Any] | list[Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self._send_security_and_cors_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(raw)
        self.wfile.flush()

    def _send_html(self, status_code: int, html_content: str) -> None:
        raw = html_content.encode("utf-8")
        self.send_response(status_code)
        self._send_security_and_cors_headers()
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(raw)
        self.wfile.flush()

    def _send_not_found(self, path: str) -> None:
        self._send_json(
            404,
            {
                "error": {
                    "code": "ERR-404-ROUTE-NOT-FOUND",
                    "type": "RouteNotFoundError",
                    "message": f"Route '{path}' was not found.",
                    "details": {"path": path},
                }
            },
        )

    def _send_method_not_allowed(self, method: str) -> None:
        self._send_json(
            405,
            {
                "error": {
                    "code": "ERR-405-METHOD-NOT-ALLOWED",
                    "type": "MethodNotAllowedError",
                    "message": f"HTTP method '{method}' is not permitted on this resource.",
                    "details": {"method": method},
                }
            },
        )

    def _read_json_body(self, *, allow_empty: bool = False) -> dict[str, Any]:
        content_length_raw = self.headers.get("Content-Length")
        if content_length_raw is None:
            if allow_empty:
                return {}
            raise MalformedJsonError("Missing Content-Length header or JSON request body.")

        try:
            content_length = int(content_length_raw)
        except ValueError as exc:
            raise MalformedJsonError("Invalid Content-Length header.") from exc

        if content_length < 0:
            raise MalformedJsonError("Negative Content-Length header is invalid.")

        if content_length > MAX_JSON_BODY_BYTES:
            raise PayloadTooLargeError(
                f"Request body of {content_length} bytes exceeds maximum limit of "
                f"{MAX_JSON_BODY_BYTES} bytes (64 KB)."
            )

        if content_length == 0:
            if allow_empty:
                return {}
            raise MalformedJsonError("Request body cannot be empty.")

        raw_bytes = self.rfile.read(content_length)
        if len(raw_bytes) > MAX_JSON_BODY_BYTES:
            raise PayloadTooLargeError("Request body exceeds 64 KB limit.")

        try:
            decoded = raw_bytes.decode("utf-8")
            parsed = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MalformedJsonError("Request body is not valid UTF-8 JSON.") from exc

        if not isinstance(parsed, dict):
            raise MalformedJsonError("JSON request payload must be an object.")

        return parsed

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self._send_security_and_cors_headers()
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()

    def do_PUT(self) -> None:  # noqa: N802
        self._send_method_not_allowed("PUT")

    def do_PATCH(self) -> None:  # noqa: N802
        self._send_method_not_allowed("PATCH")

    def do_DELETE(self) -> None:  # noqa: N802
        self._send_method_not_allowed("DELETE")

    def do_GET(self) -> None:  # noqa: N802
        parsed_url = urlparse(self.path)
        path = parsed_url.path.rstrip("/") or "/"
        qs = parse_qs(parsed_url.query, keep_blank_values=True)

        try:
            if path in ("/", "/ui", "/index.html"):
                if UI_INDEX_PATH.exists():
                    html_text = UI_INDEX_PATH.read_text(encoding="utf-8")
                else:
                    html_text = (
                        "<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'>"
                        "<title>LedgerCore Distributed</title></head>"
                        "<body><h1>LedgerCore Distributed Saga &amp; Outbox API v1</h1>"
                        "<p>Inspect <code>/api/v1/health</code> or <code>/api/v1/accounts</code>.</p>"
                        "</body></html>"
                    )
                self._send_html(200, html_text)
                return

            if path == "/api/v1/health":
                self._send_json(200, self.service.get_health())
                return

            if path == "/api/v1/accounts":
                accounts = self.service.list_accounts()
                self._send_json(200, {"accounts": accounts, "total": len(accounts)})
                return

            if path == "/api/v1/sagas":
                limit = _parse_positive_limit(qs, default=100)
                sagas = self.service.list_sagas(limit=limit)
                self._send_json(200, {"sagas": sagas, "total": len(sagas)})
                return

            if path.startswith("/api/v1/sagas/"):
                raw_saga_id = path[len("/api/v1/sagas/") :]
                if "/" in raw_saga_id or not raw_saga_id:
                    self._send_not_found(path)
                    return
                saga_id = unquote(raw_saga_id)
                saga_detail = self.service.get_saga(saga_id)
                self._send_json(200, saga_detail)
                return

            if path == "/api/v1/ledger/entries":
                limit = _parse_positive_limit(qs, default=200)
                tx_values = qs.get("transaction_id")
                transaction_id = tx_values[0].strip() if tx_values and tx_values[0].strip() else None
                entries = self.service.list_ledger_entries(
                    transaction_id=transaction_id,
                    limit=limit,
                )
                self._send_json(
                    200,
                    {
                        "entries": entries,
                        "ledger_entries": entries,
                        "total": len(entries),
                    },
                )
                return

            if path == "/api/v1/ledger/audit":
                self._send_json(200, self.service.get_zero_sum_audit())
                return

            if path == "/api/v1/outbox":
                limit = _parse_positive_limit(qs, default=100)
                events = self.service.list_outbox_events(limit=limit)
                self._send_json(
                    200,
                    {
                        "events": events,
                        "outbox_events": events,
                        "total": len(events),
                    },
                )
                return

            if path == "/api/v1/kafka/events":
                limit = _parse_positive_limit(qs, default=100)
                topic_values = qs.get("topic")
                topic = topic_values[0].strip() if topic_values and topic_values[0].strip() else None
                events = self.service.list_kafka_events(topic=topic, limit=limit)
                self._send_json(
                    200,
                    {
                        "events": events,
                        "kafka_events": events,
                        "topic": topic,
                        "total": len(events),
                    },
                )
                return

            if path == "/api/v1/dlq":
                limit = _parse_positive_limit(qs, default=50)
                events = self.service.list_dlq_events(limit=limit)
                self._send_json(
                    200,
                    {
                        "events": events,
                        "dlq_events": events,
                        "total": len(events),
                    },
                )
                return

            self._send_not_found(path)
        except Exception as exc:
            status_code, err_body = map_exception_to_http(exc)
            self._send_json(status_code, err_body)

    def do_POST(self) -> None:  # noqa: N802
        parsed_url = urlparse(self.path)
        path = parsed_url.path.rstrip("/") or "/"

        try:
            if path == "/api/v1/transfers":
                payload = self._read_json_body(allow_empty=False)
                idem_header = self.headers.get("Idempotency-Key")
                status_code, result = self.service.execute_transfer(
                    payload=payload,
                    idempotency_key_header=idem_header,
                )
                self._send_json(status_code, result)
                return

            if path == "/api/v1/outbox/relay":
                # Accept empty body or optional JSON payload (e.g. {"limit": 50})
                payload = self._read_json_body(allow_empty=True)
                limit = payload.get("limit", 100)
                if limit is None:
                    limit = 100
                if (
                    not isinstance(limit, int)
                    or isinstance(limit, bool)
                    or limit < 1
                    or limit > MAX_QUERY_LIMIT
                ):
                    raise InvalidTransferError(
                        f"Field 'limit' must be an integer between 1 and {MAX_QUERY_LIMIT}.",
                        details={"field": "limit", "value": limit},
                    )
                relayed = self.service.relay_pending_outbox_events(limit=limit)
                self._send_json(
                    200,
                    {
                        "relayed": relayed,
                        "relayed_count": len(relayed),
                    },
                )
                return

            self._send_not_found(path)
        except Exception as exc:
            status_code, err_body = map_exception_to_http(exc)
            self._send_json(status_code, err_body)


def create_http_server(
    host: str = "127.0.0.1",
    port: int = 8096,
    service: LedgerCoreApplicationService | None = None,
) -> ThreadingHTTPServer:
    """Create and configure a ThreadingHTTPServer bound to ``(host, port)``."""
    owns_service = False
    if service is None:
        sqlite_path = os.environ.get("LEDGERCORE_SQLITE_PATH", ":memory:") or ":memory:"
        service = LedgerCoreApplicationService(db_path=sqlite_path, auto_seed=True)
        owns_service = True

    return LedgerCoreThreadingHTTPServer(
        (host, port),
        LedgerCoreHTTPRequestHandler,
        service=service,
        owns_service=owns_service,
    )


def main() -> None:
    """Start the LedgerCore Distributed HTTP server from environment configuration."""
    host = os.environ.get("LEDGERCORE_HOST", "0.0.0.0")
    port = int(os.environ.get("LEDGERCORE_PORT", "8096"))
    sqlite_path = os.environ.get(
        "LEDGERCORE_SQLITE_PATH",
        "./.data/ledgercore_dev.sqlite3",
    )
    service = LedgerCoreApplicationService(db_path=sqlite_path, auto_seed=True)
    server = create_http_server(host=host, port=port, service=service)
    bound_host, bound_port = server.server_address[:2]
    print(f"LedgerCore Distributed Server listening on http://{bound_host}:{bound_port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        service.close()


if __name__ == "__main__":
    main()
