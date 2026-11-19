"""End-to-end HTTP/1.1 integration tests for the LedgerCore Distributed Saga & Outbox REST API."""

from __future__ import annotations

from http.client import HTTPConnection
import json
import os
import threading
from typing import Any
import unittest

from src.interfaces.http.server import (
    LedgerCoreThreadingHTTPServer,
    MAX_JSON_BODY_BYTES,
    create_http_server,
)


class TestHttpSagaApiIntegration(unittest.TestCase):
    """Spin up a live ThreadingHTTPServer on an ephemeral port and verify every API v1 contract."""

    def setUp(self) -> None:
        self._prev_sqlite_path = os.environ.get("LEDGERCORE_SQLITE_PATH")
        os.environ["LEDGERCORE_SQLITE_PATH"] = ":memory:"
        self.server = create_http_server("127.0.0.1", 0)
        self.host, self.port = self.server.server_address[:2]
        self.server_thread = threading.Thread(
            target=self.server.serve_forever,
            name="ledgercore-http-test-server",
            daemon=True,
        )
        self.server_thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=5)
        if self._prev_sqlite_path is None:
            os.environ.pop("LEDGERCORE_SQLITE_PATH", None)
        else:
            os.environ["LEDGERCORE_SQLITE_PATH"] = self._prev_sqlite_path

    def _request_raw(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        conn = HTTPConnection(self.host, self.port, timeout=10)
        try:
            req_headers = dict(headers) if headers else {}
            conn.request(method, path, body=body, headers=req_headers)
            resp = conn.getresponse()
            raw_body = resp.read()
            resp_headers = {k.lower(): v for k, v in resp.getheaders()}
            return resp.status, resp_headers, raw_body
        finally:
            conn.close()

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        payload: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], dict[str, Any], str]:
        req_headers = dict(headers) if headers else {}
        body: bytes | None = None
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            req_headers.setdefault("Content-Type", "application/json")
        status, resp_headers, raw = self._request_raw(
            method,
            path,
            body=body,
            headers=req_headers,
        )
        text = raw.decode("utf-8")
        data = json.loads(text)
        return status, resp_headers, data, text

    def _assert_security_headers(self, headers: dict[str, str]) -> None:
        self.assertEqual(headers.get("x-content-type-options"), "nosniff")
        self.assertEqual(headers.get("x-frame-options"), "DENY")
        self.assertEqual(headers.get("referrer-policy"), "no-referrer")
        self.assertIn("access-control-allow-origin", headers)

    def _assert_no_stack_trace(self, raw_text: str) -> None:
        forbidden_markers = (
            "Traceback (most recent call last)",
            'File "',
            ".py\", line ",
            "sqlite3.",
            "OperationalError",
        )
        for marker in forbidden_markers:
            self.assertNotIn(
                marker,
                raw_text,
                f"Detected forbidden internal trace marker '{marker}' in error payload: {raw_text}",
            )

    def _accounts_by_id(self) -> dict[str, dict[str, Any]]:
        status, _, data, _ = self._request_json("GET", "/api/v1/accounts")
        self.assertEqual(status, 200)
        return {acct["account_id"]: acct for acct in data["accounts"]}

    def test_workbench_html_health_accounts_and_cors_options(self) -> None:
        # GET / -> 200 HTML Workbench
        status, headers, raw = self._request_raw("GET", "/")
        self.assertEqual(status, 200)
        self._assert_security_headers(headers)
        self.assertIn("text/html", headers.get("content-type", ""))
        html_text = raw.decode("utf-8")
        self.assertIn("LedgerCore Distributed", html_text)

        # GET /api/v1/health -> 200 OK
        status, headers, health, _ = self._request_json("GET", "/api/v1/health")
        self.assertEqual(status, 200)
        self._assert_security_headers(headers)
        self.assertEqual(health["status"], "ok")
        self.assertEqual(health["service"], "ledgercore-distributed")
        self.assertEqual(health["accounts_count"], 5)
        self.assertTrue(health["zero_sum_balanced"])
        self.assertEqual(health["zero_sum_audit"]["imbalance_cents"], 0)

        # GET /api/v1/accounts -> 200 OK with 5 seeded accounts
        status, headers, accounts_data, _ = self._request_json("GET", "/api/v1/accounts")
        self.assertEqual(status, 200)
        self._assert_security_headers(headers)
        self.assertEqual(accounts_data["total"], 5)
        self.assertEqual(len(accounts_data["accounts"]), 5)
        by_id = {acct["account_id"]: acct["balance_cents"] for acct in accounts_data["accounts"]}
        self.assertEqual(by_id["ACC-TREASURY-001"], 50_000_000)
        self.assertEqual(by_id["ACC-MERCHANT-002"], 12_500_000)
        self.assertEqual(by_id["ACC-CUSTOMER-003"], 4_850_000)
        self.assertEqual(by_id["ACC-SETTLEMENT-004"], 8_500_000)
        self.assertEqual(by_id["ACC-ESCROW-CLEARING"], 0)

        # OPTIONS preflight -> 204 with Idempotency-Key allowed
        status, headers, _ = self._request_raw("OPTIONS", "/api/v1/transfers")
        self.assertEqual(status, 204)
        self.assertIn("Idempotency-Key", headers.get("access-control-allow-headers", ""))

    def test_normal_transfer_and_idempotent_replay_prevent_double_spend(self) -> None:
        transfer_payload = {
            "source_account_id": "ACC-TREASURY-001",
            "destination_account_id": "ACC-MERCHANT-002",
            "amount_cents": 150_000,
            "currency": "USD",
            "simulate_settlement_failure": False,
        }
        idem_headers = {"Idempotency-Key": "idem-http-settle-001"}

        # First submission -> 201 Created, SETTLED, zero-sum balanced
        status, headers, created, _ = self._request_json(
            "POST",
            "/api/v1/transfers",
            payload=transfer_payload,
            headers=idem_headers,
        )
        self.assertEqual(status, 201)
        self._assert_security_headers(headers)
        self.assertFalse(created["idempotent_replay"])
        self.assertEqual(created["saga"]["status"], "SETTLED")
        self.assertGreaterEqual(created["saga"]["fencing_token"], 1)
        self.assertEqual(len(created["ledger_entries"]), 4)
        self.assertEqual(len(created["outbox_relayed"]), 2)
        self.assertTrue(created["zero_sum_audit"]["is_balanced"])
        self.assertEqual(created["zero_sum_audit"]["imbalance_cents"], 0)

        saga_id = created["saga"]["saga_id"]
        accounts_after_first = self._accounts_by_id()
        self.assertEqual(accounts_after_first["ACC-TREASURY-001"]["balance_cents"], 49_850_000)
        self.assertEqual(accounts_after_first["ACC-MERCHANT-002"]["balance_cents"], 12_650_000)
        self.assertEqual(accounts_after_first["ACC-ESCROW-CLEARING"]["balance_cents"], 0)

        # Exact Idempotency-Key + payload replay -> 200 OK, idempotent_replay=True, zero double-spend
        replay_status, _, replayed, _ = self._request_json(
            "POST",
            "/api/v1/transfers",
            payload=transfer_payload,
            headers=idem_headers,
        )
        self.assertEqual(replay_status, 200)
        self.assertTrue(replayed["idempotent_replay"])
        self.assertEqual(replayed["saga"]["saga_id"], saga_id)
        self.assertEqual(replayed["outbox_relayed"], [])
        self.assertTrue(replayed["zero_sum_audit"]["is_balanced"])

        # Balances and saga count must not drift on replay
        accounts_after_replay = self._accounts_by_id()
        self.assertEqual(accounts_after_replay["ACC-TREASURY-001"]["balance_cents"], 49_850_000)
        self.assertEqual(accounts_after_replay["ACC-MERCHANT-002"]["balance_cents"], 12_650_000)

        # Inspect saga, ledger entries, outbox, kafka events, and manual relay sweep
        s_status, _, saga_detail, _ = self._request_json("GET", f"/api/v1/sagas/{saga_id}")
        self.assertEqual(s_status, 200)
        self.assertEqual(saga_detail["saga_id"], saga_id)
        self.assertEqual(len(saga_detail["ledger_entries"]), 4)

        list_status, _, sagas_list, _ = self._request_json("GET", "/api/v1/sagas")
        self.assertEqual(list_status, 200)
        self.assertEqual(sagas_list["total"], 1)

        ent_status, _, entries_data, _ = self._request_json(
            "GET",
            f"/api/v1/ledger/entries?transaction_id={saga_id}",
        )
        self.assertEqual(ent_status, 200)
        self.assertEqual(entries_data["total"], 4)

        outbox_status, _, outbox_data, _ = self._request_json("GET", "/api/v1/outbox")
        self.assertEqual(outbox_status, 200)
        self.assertEqual(outbox_data["total"], 2)
        self.assertTrue(all(e["status"] == "PUBLISHED" for e in outbox_data["events"]))

        kafka_status, _, kafka_data, _ = self._request_json("GET", "/api/v1/kafka/events")
        self.assertEqual(kafka_status, 200)
        self.assertEqual(
            [e["topic"] for e in kafka_data["events"]],
            ["ledger.transfers.reserved", "ledger.transfers.settled"],
        )

        relay_status, _, relay_data, _ = self._request_json("POST", "/api/v1/outbox/relay", payload={})
        self.assertEqual(relay_status, 200)
        self.assertEqual(relay_data["relayed_count"], 0)

    def test_reused_idempotency_key_with_different_amount_returns_409_conflict(self) -> None:
        headers = {"Idempotency-Key": "idem-http-conflict-001"}
        first_payload = {
            "source_account_id": "ACC-TREASURY-001",
            "destination_account_id": "ACC-MERCHANT-002",
            "amount_cents": 50_000,
            "currency": "USD",
        }
        status1, _, _, _ = self._request_json(
            "POST",
            "/api/v1/transfers",
            payload=first_payload,
            headers=headers,
        )
        self.assertEqual(status1, 201)

        mutated_payload = {
            "source_account_id": "ACC-TREASURY-001",
            "destination_account_id": "ACC-MERCHANT-002",
            "amount_cents": 75_000,
            "currency": "USD",
        }
        status2, resp_headers, err_data, raw_text = self._request_json(
            "POST",
            "/api/v1/transfers",
            payload=mutated_payload,
            headers=headers,
        )
        self.assertEqual(status2, 409)
        self._assert_security_headers(resp_headers)
        self._assert_no_stack_trace(raw_text)
        self.assertEqual(err_data["error"]["code"], "ERR-409-IDEMPOTENCY-CONFLICT")
        self.assertEqual(err_data["error"]["type"], "IdempotencyConflictError")

        # Only the first 50_000 transfer was applied.
        accounts = self._accounts_by_id()
        self.assertEqual(accounts["ACC-TREASURY-001"]["balance_cents"], 49_950_000)
        self.assertEqual(accounts["ACC-MERCHANT-002"]["balance_cents"], 12_550_000)

    def test_insufficient_funds_returns_422_unprocessable_entity(self) -> None:
        payload = {
            "idempotency_key": "idem-http-overdraft-001",
            "source_account_id": "ACC-CUSTOMER-003",
            "destination_account_id": "ACC-SETTLEMENT-004",
            "amount_cents": 9_999_999,
            "currency": "USD",
        }
        status, resp_headers, err_data, raw_text = self._request_json(
            "POST",
            "/api/v1/transfers",
            payload=payload,
        )
        self.assertEqual(status, 422)
        self._assert_security_headers(resp_headers)
        self._assert_no_stack_trace(raw_text)
        self.assertEqual(err_data["error"]["code"], "ERR-422-INSUFFICIENT-FUNDS")
        self.assertEqual(err_data["error"]["details"]["available_cents"], 4_850_000)
        self.assertEqual(err_data["error"]["details"]["requested_cents"], 9_999_999)

        # Verify zero side effects persisted.
        accounts = self._accounts_by_id()
        self.assertEqual(accounts["ACC-CUSTOMER-003"]["balance_cents"], 4_850_000)
        _, _, sagas_data, _ = self._request_json("GET", "/api/v1/sagas")
        self.assertEqual(sagas_data["total"], 0)

    def test_simulated_settlement_failure_compensates_publishes_dlq_and_preserves_zero_sum(self) -> None:
        initial_source = self._accounts_by_id()["ACC-CUSTOMER-003"]["balance_cents"]
        initial_dest = self._accounts_by_id()["ACC-SETTLEMENT-004"]["balance_cents"]

        payload = {
            "source_account_id": "ACC-CUSTOMER-003",
            "destination_account_id": "ACC-SETTLEMENT-004",
            "amount_cents": 200_000,
            "currency": "USD",
            "simulate_settlement_failure": True,
        }
        status, resp_headers, result, _ = self._request_json(
            "POST",
            "/api/v1/transfers",
            payload=payload,
            headers={"Idempotency-Key": "idem-http-fail-comp-001"},
        )
        self.assertEqual(status, 201)
        self._assert_security_headers(resp_headers)
        self.assertFalse(result["idempotent_replay"])
        self.assertEqual(result["saga"]["status"], "COMPENSATED")
        self.assertTrue(result["saga"]["failure_reason"])
        self.assertIn("dlq_event", result)
        self.assertEqual(len(result["ledger_entries"]), 4)
        self.assertEqual(
            [e["entry_type"] for e in result["ledger_entries"]],
            ["TRANSFER_RESERVE", "TRANSFER_RESERVE", "SAGA_COMPENSATION", "SAGA_COMPENSATION"],
        )

        # Source balance fully restored, destination untouched, escrow empty.
        accounts = self._accounts_by_id()
        self.assertEqual(accounts["ACC-CUSTOMER-003"]["balance_cents"], initial_source)
        self.assertEqual(accounts["ACC-SETTLEMENT-004"]["balance_cents"], initial_dest)
        self.assertEqual(accounts["ACC-ESCROW-CLEARING"]["balance_cents"], 0)

        # Record present in /api/v1/dlq.
        dlq_status, _, dlq_data, _ = self._request_json("GET", "/api/v1/dlq")
        self.assertEqual(dlq_status, 200)
        self.assertEqual(dlq_data["total"], 1)
        self.assertEqual(dlq_data["events"][0]["topic"], "ledger.transfers.dlq")
        self.assertEqual(dlq_data["events"][0]["dlq_id"], result["dlq_event"]["dlq_id"])

        # Events present in /api/v1/kafka/events including reserved, compensated, and dlq.
        kafka_status, _, kafka_data, _ = self._request_json("GET", "/api/v1/kafka/events")
        self.assertEqual(kafka_status, 200)
        topics = [evt["topic"] for evt in kafka_data["events"]]
        self.assertIn("ledger.transfers.reserved", topics)
        self.assertIn("ledger.transfers.compensated", topics)
        self.assertIn("ledger.transfers.dlq", topics)

        dlq_kafka_status, _, dlq_kafka_data, _ = self._request_json(
            "GET",
            "/api/v1/kafka/events?topic=ledger.transfers.dlq",
        )
        self.assertEqual(dlq_kafka_status, 200)
        self.assertEqual(dlq_kafka_data["total"], 1)

        # /api/v1/ledger/audit reports balanced zero-sum accounting (imbalance_cents == 0).
        audit_status, _, audit_data, _ = self._request_json("GET", "/api/v1/ledger/audit")
        self.assertEqual(audit_status, 200)
        self.assertTrue(audit_data["is_balanced"])
        self.assertEqual(audit_data["imbalance_cents"], 0)
        self.assertEqual(audit_data["total_entries"], 4)

    def test_invalid_payloads_missing_resources_disallowed_methods_and_zero_stack_traces(self) -> None:
        # 400 Bad Request cases: float amount, bool amount, self-transfer, missing key, malformed JSON, bad query limit
        invalid_payloads: list[tuple[str, dict[str, Any]]] = [
            (
                "float_amount",
                {
                    "idempotency_key": "idem-bad-1",
                    "source_account_id": "ACC-TREASURY-001",
                    "destination_account_id": "ACC-MERCHANT-002",
                    "amount_cents": 12.34,
                },
            ),
            (
                "bool_amount",
                {
                    "idempotency_key": "idem-bad-2",
                    "source_account_id": "ACC-TREASURY-001",
                    "destination_account_id": "ACC-MERCHANT-002",
                    "amount_cents": True,
                },
            ),
            (
                "self_transfer",
                {
                    "idempotency_key": "idem-bad-3",
                    "source_account_id": "ACC-TREASURY-001",
                    "destination_account_id": "ACC-TREASURY-001",
                    "amount_cents": 1_000,
                },
            ),
            (
                "missing_idempotency_key",
                {
                    "source_account_id": "ACC-TREASURY-001",
                    "destination_account_id": "ACC-MERCHANT-002",
                    "amount_cents": 1_000,
                },
            ),
        ]
        for label, bad_payload in invalid_payloads:
            with self.subTest(case=label):
                status, _, err_data, raw_text = self._request_json(
                    "POST",
                    "/api/v1/transfers",
                    payload=bad_payload,
                )
                self.assertEqual(status, 400)
                self.assertEqual(err_data["error"]["code"], "ERR-400-INVALID-TRANSFER")
                self._assert_no_stack_trace(raw_text)

        # Malformed JSON -> 400
        status, _, raw = self._request_raw(
            "POST",
            "/api/v1/transfers",
            body=b"{not-valid-json",
            headers={"Content-Type": "application/json"},
        )
        raw_text = raw.decode("utf-8")
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(raw_text)["error"]["code"], "ERR-400-MALFORMED-JSON")
        self._assert_no_stack_trace(raw_text)

        # Invalid query parameter -> 400
        status, _, err_data, raw_text = self._request_json("GET", "/api/v1/sagas?limit=notanint")
        self.assertEqual(status, 400)
        self.assertEqual(err_data["error"]["code"], "ERR-400-INVALID-TRANSFER")
        self._assert_no_stack_trace(raw_text)

        # Missing Saga -> 404
        status, _, err_data, raw_text = self._request_json("GET", "/api/v1/sagas/SAGA-DOES-NOT-EXIST")
        self.assertEqual(status, 404)
        self.assertEqual(err_data["error"]["code"], "ERR-404-ACCOUNT-NOT-FOUND")
        self._assert_no_stack_trace(raw_text)

        # Unknown account in transfer -> 404
        status, _, err_data, raw_text = self._request_json(
            "POST",
            "/api/v1/transfers",
            payload={
                "idempotency_key": "idem-missing-acct",
                "source_account_id": "ACC-NONEXISTENT-999",
                "destination_account_id": "ACC-MERCHANT-002",
                "amount_cents": 1_000,
            },
        )
        self.assertEqual(status, 404)
        self.assertEqual(err_data["error"]["code"], "ERR-404-ACCOUNT-NOT-FOUND")
        self._assert_no_stack_trace(raw_text)

        # Unknown route -> 404
        status, _, err_data, raw_text = self._request_json("GET", "/api/v1/does-not-exist")
        self.assertEqual(status, 404)
        self.assertEqual(err_data["error"]["code"], "ERR-404-ROUTE-NOT-FOUND")
        self._assert_no_stack_trace(raw_text)

        # Append-only ledger HTTP guard: DELETE / PUT / PATCH -> 405 Method Not Allowed
        for disallowed_method in ("DELETE", "PUT", "PATCH"):
            with self.subTest(method=disallowed_method):
                status, _, err_data, raw_text = self._request_json(
                    disallowed_method,
                    "/api/v1/ledger/entries",
                )
                self.assertEqual(status, 405)
                self.assertEqual(err_data["error"]["code"], "ERR-405-METHOD-NOT-ALLOWED")
                self._assert_no_stack_trace(raw_text)

        # Payload exceeding MAX_JSON_BODY_BYTES (64 KB) -> 413 Payload Too Large
        oversized_body = b"{" + (b" " * (MAX_JSON_BODY_BYTES + 128)) + b"}"
        status, _, raw = self._request_raw(
            "POST",
            "/api/v1/transfers",
            body=oversized_body,
            headers={"Content-Type": "application/json"},
        )
        raw_text = raw.decode("utf-8")
        self.assertEqual(status, 413)
        self.assertEqual(json.loads(raw_text)["error"]["code"], "ERR-413-PAYLOAD-TOO-LARGE")
        self._assert_no_stack_trace(raw_text)

        # Distributed lock contention -> 409 Conflict (ERR-409-LOCK-CONTENTION)
        assert isinstance(self.server, LedgerCoreThreadingHTTPServer)
        lock_mgr = self.server.service.lock_manager
        lock_mgr.acquire("ACC-TREASURY-001", owner="SAGA-EXTERNAL-LOCK")
        try:
            status, _, err_data, raw_text = self._request_json(
                "POST",
                "/api/v1/transfers",
                payload={
                    "idempotency_key": "idem-lock-contention-001",
                    "source_account_id": "ACC-TREASURY-001",
                    "destination_account_id": "ACC-MERCHANT-002",
                    "amount_cents": 1_000,
                },
            )
            self.assertEqual(status, 409)
            self.assertEqual(err_data["error"]["code"], "ERR-409-LOCK-CONTENTION")
            self._assert_no_stack_trace(raw_text)
        finally:
            lock_mgr.release("ACC-TREASURY-001", owner="SAGA-EXTERNAL-LOCK")


if __name__ == "__main__":
    unittest.main()
