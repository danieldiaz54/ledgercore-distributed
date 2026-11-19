# SPEC-04: Formal Security Certification & OWASP Top 10 Audit Report

> **Document ID**: `SEC-AUDIT-LEDGERCORE-001`  
> **Status**: **CERTIFIED (PASS WITH HARDENING RECOMMENDATIONS)**  
> **Audit Date**: `2026-11-19`  
> **Auditor**: `security-auditor` (DevSecOps & Application Security)  
> **Repository**: `ledgercore-distributed`  
> **Branch Audited**: `feat/saga-outbox-distributed-ledger` (ancestry verified from `dev` commit `08f07e0`)  
> **Certification Verdict**: **APPROVED FOR `dev` INTEGRATION & PORTFOLIO RELEASE** *(0 Critical, 0 High in target showcase/sandbox runtime; Production Deployment Gates documented in Section 4)*

---

## 1. Executive Summary & Scope

This specification documents the formal static, AST, configuration, and dynamic in-memory security audit of **`ledgercore-distributed`** — a Distributed Double-Entry Accounting Engine with Orchestrated Sagas, Transactional Outbox Relay, SHA-256 Idempotency Keys, Monotonic Lock Fencing, Dead-Letter Queue (DLQ) Compensation, and a Java 21 / Spring Boot 3.3 enterprise microservice blueprint.

### Audited Artifacts

| Layer | Inspected Files |
|---|---|
| **Pure Domain Core** | `src/domain/exceptions.py`, `src/domain/ledger.py`, `src/domain/repositories.py`, `src/domain/saga_outbox.py` |
| **Application Orchestration** | `src/application/services.py` |
| **Persistence & Migrations** | `src/infrastructure/persistence/sqlite_ledger_repo.py`, `src/infrastructure/persistence/seed_accounts.py`, `migrations/001_init_ledgercore_outbox.sql` |
| **HTTP Interface Boundary** | `src/interfaces/http/server.py`, `src/main.py` |
| **Java 21 / Spring Boot 3 Blueprint** | `java-service/pom.xml`, `LedgerEntry.java`, `OutboxEvent.java`, `TransferSagaOrchestrator.java`, `TransferController.java` |
| **Frontend Workbench UI** | `ui/index.html` (all 4,019 lines) |
| **Containers, CI & Governance** | `Dockerfile`, `docker-compose.yml`, `docker-compose.dev.yml`, `.gitignore`, `.env.development.example`, `.env.production.example`, `.github/workflows/ci.yml`, `.agents/hooks.json`, `.agents/scripts/safety-gate.py`, `.agents/scripts/safety-gate.js`, `docs/adr/*`, `tests/*` |

### Summary Scorecard

| Security Control Domain | Result | Critical | High | Medium | Low / Info |
|---|:---:|:---:|:---:|:---:|:---:|
| 1. SQL Injection Prevention & Double-Entry Immutability | **PASS** | 0 | 0 | 0 | 2 |
| 2. Financial Arithmetic, Idempotency & Lock Fencing | **PASS** | 0 | 0 | 2 | 2 |
| 3. XSS & DOM Safety (`ui/index.html`) | **PASS** | 0 | 0 | 0 | 1 |
| 4. HTTP Boundary Hardening & Error Sanitization | **PASS** | 0 | 0 | 2 | 2 |
| 5. Container Least Privilege, Secrets & Safety Gate | **PASS** | 0 | 0 | 1 | 3 |
| **Total** | **CERTIFIED** | **0** | **0** | **5** | **10** |

> **Note on Severity Framing**: Medium findings represent **pre-production hardening requirements** (e.g., adding JWT/OAuth2 + RBAC authentication before exposing financial routes on a public network, replacing wildcard CORS in production, switching Java SHA-256 canonicalization from `String.format` to `ObjectMapper`, and completing storage-side fencing verification in multi-node PostgreSQL). None block integration to `dev` or local/containerized portfolio demonstration.

---

## 2. Detailed Control Verification Matrix

### 2.1 SQL Injection Prevention & Double-Entry Immutability — **PASS**

| Control | Evidence & Line References | Empirical Verification |
|---|---|---|
| **100% Parameterized Queries (`?`)** | `src/infrastructure/persistence/sqlite_ledger_repo.py` (lines 265–1105) | Python `ast` walk across all `src/**/*.py` inspected **40** SQL execution sites: **31** parameterized queries using static string literals + `?` binding, **7** zero-parameter static statements (`BEGIN IMMEDIATE`, `COMMIT`, `ROLLBACK`, `PRAGMA`, global audit aggregates), **1** static DDL `executescript(ddl_script)` (line 173), and **1** test-only helper `execute_raw_mutation_for_test` (line 1151, unreachable from HTTP/application layers). Zero string interpolation (`f""`, `%`, `.format()`) in SQL queries. |
| **SQLi Attack Probes** | `POST /api/v1/transfers`, `GET /api/v1/ledger/entries?transaction_id=...`, `GET /api/v1/sagas/{saga_id}` | Dynamic probes injecting `' OR '1'='1'; --` returned `404 ERR-404-ACCOUNT-NOT-FOUND`, `total=0`, and `404` respectively with zero SQL errors or unauthorized data disclosure. |
| **SQLite Append-Only Triggers** | `src/infrastructure/persistence/sqlite_ledger_repo.py:160-170` | Verified `trg_ledger_entries_no_update` (`BEFORE UPDATE ON ledger_entries`) and `trg_ledger_entries_no_delete` (`BEFORE DELETE ON ledger_entries`) raising `ABORT` with `APPEND_ONLY_LEDGER_VIOLATION`. Dynamic probe confirmed both `UPDATE` and `DELETE` raise `AppendOnlyLedgerError`. |
| **PostgreSQL Append-Only Triggers** | `migrations/001_init_ledgercore_outbox.sql:110-147` | Verified PL/pgSQL function `prevent_ledger_entry_mutation()` raising `APPEND_ONLY_LEDGER_VIOLATION` (`ERRCODE = 'check_violation'`), bound idempotently via `BEFORE UPDATE` (`trg_ledger_entries_no_update`) and `BEFORE DELETE` (`trg_ledger_entries_no_delete`) `FOR EACH ROW` on `ledger_entries`. |
| **ORM-Layer Immutability (Java)** | `java-service/.../domain/LedgerEntry.java:25-61` | Annotated with `@Entity`, `@Immutable`, `updatable = false` on all columns, and zero mutator/setter methods. |
| **Referential & Constraint Integrity** | `sqlite_ledger_repo.py:70-146`, `001_init_ledgercore_outbox.sql:7-84` | `PRAGMA foreign_keys = ON`, `CHECK (balance_cents >= 0)`, `CHECK (amount_cents > 0)`, `CHECK (source_account_id <> destination_account_id)`, `CHECK (direction IN ('DEBIT', 'CREDIT'))`, and `UNIQUE (idempotency_key)`. |

---

### 2.2 Financial Arithmetic, Idempotency & Distributed Lock Fencing — **PASS**

| Control | Evidence & Line References | Empirical Verification |
|---|---|---|
| **Exact Integer-Cent Arithmetic (Zero Float Rounding)** | `src/domain/ledger.py:39-49`, `src/domain/saga_outbox.py:48-58`, `src/application/services.py:136-150`, `LedgerEntry.java:51-52,155-159`, `ui/index.html:1936-1981` | Python `_require_exact_int()` rejects `float`, `str`, and `bool` (`isinstance(value, int) and not isinstance(value, bool)`). Dynamic HTTP probes confirmed `100.5`, `100.0`, `true`, `0`, `-50`, and `"100"` all return `400 ERR-400-INVALID-TRANSFER`; extreme values (`10**30`) fail overdraft validation cleanly with `422 ERR-422-INSUFFICIENT-FUNDS` prior to SQLite write. Java `LedgerEntry.validateBalanced()` uses `long` + `Math.addExact()` to trap 64-bit overflow. |
| **Zero-Sum Double-Entry Invariant** | `src/domain/ledger.py:141-220`, `sqlite_ledger_repo.py:502,677,825,1083-1137`, `LedgerEntry.java:137-173` | Every `RESERVE`, `SETTLE`, and `COMPENSATE` transaction validates `SUM(DEBIT) == SUM(CREDIT)` per currency with `>= 2` entries before SQL insertion. |
| **SHA-256 Request Fingerprinting (Idempotency Reuse Defense)** | `src/domain/saga_outbox.py:158-179`, `src/application/services.py:172-238`, `sqlite_ledger_repo.py:443-456` | Canonical JSON (`sort_keys=True, separators=(",", ":")`) hashed via `hashlib.sha256`. Checked prior to lock, re-verified inside lock, and constrained by `idempotency_records` PK. Dynamic probes confirmed: first transfer `201 Created`, identical replay `200 OK` (`idempotent_replay: true`, zero extra postings), tampered amount under same key `409 Conflict` (`ERR-409-IDEMPOTENCY-CONFLICT`). |
| **Distributed Lock Fencing Tokens** | `src/domain/saga_outbox.py:340-456`, `src/application/services.py:232,297`, `TransferSagaOrchestrator.java:50-56,264-275` | Python `DistributedLockManager` uses `threading.Lock()`, `time.monotonic()` TTL eviction, strictly monotonic `_fencing_counter`, and owner-verified `release()` (blocking foreign lock release). Dynamic probe confirmed competing transfer returns `409 ERR-409-LOCK-CONTENTION` and unauthorized release is rejected. Java service uses Redis `SET NX` with 30 s TTL, `INCR` monotonic fencing token, and atomic Lua compare-and-delete release script. |
| **Optimistic Concurrency on Account Balances** | `sqlite_ledger_repo.py:507-531,679-713,827-861` | All balance mutations execute inside `BEGIN IMMEDIATE` with `WHERE account_id = ? AND version = ?` and `rowcount == 1` verification. |

---

### 2.3 XSS & DOM Safety (`ui/index.html`) — **PASS**

| Control | Evidence & Line References | Empirical Verification |
|---|---|---|
| **Zero Unsafe HTML Injection Sinks** | `ui/index.html` (lines 1–4019) | Full-file automated scan confirmed **0 occurrences** of `innerHTML`, `outerHTML`, `insertAdjacentHTML`, `document.write`, `eval(`, `new Function`, `srcdoc`, `javascript:`, `parseFloat`, or inline `onclick=` / `onerror=` / `onload=` / `onmouseover=` attributes. |
| **Safe DOM Construction Helpers** | `ui/index.html:2834-2905,3044-3293` | All dynamic table cells, badges, inspector metadata, and JSON response previews are built using `document.createElement()`, `node.textContent`, `node.className`, `setAttribute('colspan', ...)`, and `replaceChildren()`. API strings cannot break out into HTML/JS execution contexts. |
| **Strict Numeric Input Parsing (`parseUsdAmountToCents`)** | `ui/index.html:1936-1981` | Pure string validator rejecting non-strings, empty strings, leading `-` or `+`, scientific notation (`e`/`E`), `NaN`/`Infinity`, `>2` decimal digits (`/^\d+\.\d{3,}$/`), trailing dots (`/^\d+\.$/`), and anything outside `/^\d+(\.\d{1,2})?$/`. Parses integer dollars and padded 2-digit cents separately via `Number.parseInt(..., 10)`, verifies `Number.isSafeInteger(...)`, and bounds `1 <= cents <= 9,999,999,999` (`$99,999,999.99`). |
| **Offline-Capable Pure SHA-256** | `ui/index.html:1989-2092` | Deterministic FIPS 180-4 SHA-256 over `Uint32Array` & `TextEncoder` matching Python `TransferCommand.compute_fingerprint()` without external CDN scripts. |

---

### 2.4 HTTP Boundary Hardening & Error Sanitization — **PASS**

| Control | Evidence & Line References | Empirical Verification |
|---|---|---|
| **64 KB Strict Body Limit (`413`)** | `src/interfaces/http/server.py:27,286-325` | `MAX_JSON_BODY_BYTES = 64 * 1024`. Checked against `Content-Length` header AND post-read byte count. Missing/negative/non-integer `Content-Length` rejected with `400 ERR-400-MALFORMED-JSON`. Dynamic probe with 65,553-byte body returned `413 ERR-413-PAYLOAD-TOO-LARGE`. |
| **Security Response Headers** | `src/interfaces/http/server.py:210-211,225-258` | Verified empirically on both JSON (`/api/v1/health`) and HTML (`/`) responses: `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`, `Cache-Control: no-store` (JSON), and `Server: LedgerCore/1.0` (Python runtime `sys_version` suppressed). |
| **HTTP Verb Restriction (`405`)** | `src/interfaces/http/server.py:273-284,334-341` | `do_PUT`, `do_PATCH`, and `do_DELETE` return `405 ERR-405-METHOD-NOT-ALLOWED` across all routes (including `/api/v1/ledger/entries`). Verified empirically for all three verbs. |
| **Query Bound Enforcement** | `src/interfaces/http/server.py:28,160-179,478-487` | `_parse_positive_limit()` and `/api/v1/outbox/relay` strictly enforce `1 <= limit <= 500`. Probes with `limit=0` and `limit=501` returned `400 ERR-400-INVALID-TRANSFER`. |
| **Zero Stack-Trace Leakage** | `src/interfaces/http/server.py:41-157,453-455,499-501`, `TransferController.java:77-160` | All exceptions caught and serialized into uniform JSON `error` envelopes. Unhandled runtime exceptions return generic `500 ERR-500-INTERNAL-ERROR` (`"An unexpected internal server error occurred."`, `details: {}`). Verified empirically that internal messages/paths/tracebacks never appear in HTTP payloads. |
| **Jakarta Bean Validation (Java)** | `java-service/.../TransferController.java:51,177-199` | `@Valid @RequestBody CreateTransferHttpRequest` enforces `@NotBlank`, `@Positive`, and `@Pattern(regexp = "^[A-Z]{3}$")` with sanitized `MethodArgumentNotValidException` mapping (`400`). |

---

### 2.5 Container Least Privilege, Secret Hygiene & Safety Gate — **PASS**

| Control | Evidence & Line References | Empirical Verification |
|---|---|---|
| **Non-Root Container Execution** | `Dockerfile:15-42` | Multi-stage `python:3.12-slim` build creates system group/user `ledgercore`, sets `--chown=ledgercore:ledgercore` on `/app` and `/app/.data`, and runs `USER ledgercore` prior to `EXPOSE`, `HEALTHCHECK`, and `CMD`. Zero third-party pip packages installed at runtime (minimal CVE surface). |
| **Selective Build Artifact Copy** | `Dockerfile:8-11,30-33` | Copies only `pyproject.toml`, `requirements.txt`, `src/`, `ui/`, and `migrations/`. Never copies `.env*`, `.git/`, or `Docs/`. |
| **`.gitignore` Coverage** | `.gitignore:1-13` | Verified via `git check-ignore -v`: `.env.development` (line 9), `.env.production` (line 10), `Docs/*` and `docs/*` including `docs/README.local.md` (lines 3–4), and `.agents/.agents/` (line 13) are ignored, while `docs/adr/**` is explicitly preserved (`!/docs/adr/**`, line 6). |
| **Zero Leaked Secrets & Env Isolation** | `git ls-files`, `.env.*.example`, `docker-compose*.yml`, `tests/*` | Regex scan across 39 tracked/untracked files found **0 secrets** (0 AWS keys, 0 private keys, 0 JWTs, 0 real passwords). `.env.production.example` uses `__PROD_*__` placeholders with mandatory TLS (`sslmode=require`, `rediss://`, Kafka `:9093`). `docker-compose.dev.yml` loads only `.env.development` and mounts `./migrations` read-only (`:ro`). |
| **Safety Gate Hook Governance** | `.agents/hooks.json:1-16`, `.agents/scripts/safety-gate.py`, `.agents/scripts/safety-gate.js` | `"safety-gate": {"enabled": true}` on `PreToolUse` (`run_command`). Empirical execution confirmed `deny` on `DROP DATABASE`, `TRUNCATE TABLE`, `rm -rf /`, and `git push --force origin main`, and `allow` on safe `git status`. |
| **CI Pipeline Least Privilege** | `.github/workflows/ci.yml:9-10` | Explicitly restricts `GITHUB_TOKEN` via `permissions: { contents: read }` and enforces AST domain-purity checks. |
| **GitFlow Compliance** | `git branch -vv`, `git log` | Branch `feat/saga-outbox-distributed-ledger` originates directly from `dev` (`08f07e0`); `main` (`4f21eb1`) remains untouched. |

---

## 3. OWASP Top 10 (2021) Compliance Mapping

| OWASP Category | Status | Assessment Summary |
|---|:---:|---|
| **A01: Broken Access Control** | **Conditional** | Read-only ledger integrity enforced (`405` on `PUT`/`PATCH`/`DELETE`, owner-checked lock release). **Pre-Prod Gate (M-01)**: Add JWT/OAuth2 + RBAC guards to REST routes and Spring Actuator before public deployment. |
| **A02: Cryptographic Failures** | **PASS** | FIPS-compliant SHA-256 fingerprinting across Python/JS/Java; `.env.production.example` enforces TLS (`sslmode=require`, `rediss://`, Kafka `9093`); zero hardcoded production secrets. |
| **A03: Injection** | **PASS** | 100% parameterized `?` SQL queries (AST-verified); zero HTML/JS injection sinks in `ui/index.html`; strict input typing (`int` non-bool, ISO currency regex). See **M-02** for Java fingerprint JSON serialization. |
| **A04: Insecure Design** | **PASS** | Atomic Transactional Outbox eliminates dual-write drift; double-entry zero-sum invariant enforced in domain + DB triggers; append-only compensation + DLQ quarantine. See **M-03** for multi-node fencing verification. |
| **A05: Security Misconfiguration** | **Conditional** | Security headers (`nosniff`, `DENY`, `no-referrer`, `no-store`), `64 KB` body cap, sanitized server banner, non-root Docker user. See **M-04** (production CORS wildcard + CSP) and **M-05** (`.gitignore` / `.dockerignore`). |
| **A06: Vulnerable & Outdated Components** | **PASS** | Python runtime uses **zero** third-party runtime dependencies (100% Python 3.12 stdlib). Java blueprint targets current Spring Boot `3.3.5` / Java `21`. Recommend pinning container image digests (**L-07**). |
| **A07: Identification & Authentication Failures** | **Conditional** | No session/credential flaws exist in sandbox mode; requires OIDC/JWT token validation & per-account ownership checks prior to production exposure (**M-01**). |
| **A08: Software & Data Integrity Failures** | **PASS** | Database `BEFORE UPDATE`/`DELETE` immutability triggers, SHA-256 payload fingerprinting, optimistic version checks, CI domain-purity AST gate, and `safety-gate` PreToolUse hook. |
| **A09: Security Logging & Monitoring Failures** | **PASS (Dev)** | Structured DLQ archiving, outbox delivery telemetry, and zero-sum audit endpoint (`/api/v1/ledger/audit`). Recommend structured JSON audit logging (with correlation IDs, redacting PII) for production (**L-06**). |
| **A10: Server-Side Request Forgery (SSRF)** | **PASS** | Backend performs zero outbound user-controlled HTTP/URL fetches. |

---

## 4. Findings & Remediation Roadmap (Pre-Production Gates & Hardening)

No **Critical** or **High** vulnerabilities were identified for the current portfolio/development runtime. The following **Medium**, **Low**, and **Informational** findings are recorded for prioritized hardening prior to production deployment:

### Medium Findings (Pre-Production Deployment Gates)

| ID | Category | Location | Finding | Recommended Remediation |
|---|---|---|---|---|
| **M-01** | OWASP A01 / A07 (Auth & RBAC) | `src/interfaces/http/server.py`, `java-service/pom.xml`, `TransferController.java` | Financial mutation endpoints (`POST /api/v1/transfers`, `POST /api/v1/outbox/relay`) and ledger read/DLQ endpoints currently lack authentication/RBAC guards; `java-service/pom.xml` includes `spring-boot-starter-actuator` without `spring-boot-starter-security`. | Before any non-local deployment, enforce Bearer JWT/OAuth2 authentication + role-based scopes (`ledger:transfer:write`, `ledger:audit:read`, `ops:outbox:relay`), verify caller authorization over `source_account_id`, and lock down Actuator endpoints (`/actuator/health` only, others authenticated). |
| **M-02** | Input Canonicalization (Java) | `java-service/.../TransferSagaOrchestrator.java:377-386` | `TransferCommand.computeSha256Fingerprint()` builds canonical JSON via `String.format(...)` without JSON escaping on `sourceAccountId` / `destinationAccountId` (unlike Python `json.dumps` and JS `JSON.stringify`). Crafted quotes/backslashes could break canonical JSON or cause hash divergence across runtimes. | Serialize a sorted `TreeMap` using Jackson `ObjectMapper` (with `SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS`) AND enforce `@Pattern(regexp = "^[A-Za-z0-9_-]{1,64}$")` on `sourceAccountId`, `destinationAccountId`, and `idempotencyKey`. |
| **M-03** | Distributed Fencing & Race Safety | `src/infrastructure/persistence/sqlite_ledger_repo.py:628-872`, `TransferSagaOrchestrator.java:79-95,264-271` | (a) `settle_saga_with_outbox` and `compensate_saga_with_outbox` do not enforce storage-side fencing token validation (`AND fencing_token = ?` / monotonic token guard), and Python `lock_manager.acquire` omits a default `ttl_seconds`. (b) Java `executeTransfer()` checks idempotency only before acquiring the Redis lock (not re-checked under lock), and `setIfAbsent` + `increment` are two separate Redis calls. | Persist a monotonic `last_fencing_token` per account/saga and enforce `fencing_token >= last_fencing_token` in SQL `WHERE` clauses; re-check idempotency inside the Redis lock in Java; combine Redis lock acquisition + `INCR` into a single atomic Lua script. |
| **M-04** | CORS & Browser Hardening | `src/interfaces/http/server.py:226-236` | `Access-Control-Allow-Origin` defaults to `*` regardless of `LEDGERCORE_ENV`, and HTML responses omit `Content-Security-Policy` (CSP) and `Strict-Transport-Security` (HSTS). | Require an explicit comma-separated origin allowlist when `LEDGERCORE_ENV=production` (never `*` with credentials); emit `Content-Security-Policy` on `_send_html` responses and `Strict-Transport-Security` behind TLS termination. |
| **M-05** | `.gitignore` & `.dockerignore` Hygiene | `.gitignore`, *(missing `.dockerignore`)* | `.gitignore` excludes `.env.development` and `.env.production`, but does **not** exclude bare `.env`, `.env.local`, `.data/`, `*.sqlite3*`, or `__pycache__/`; repository also lacks a `.dockerignore` file. | Append `.env`, `.env.*.local`, `.data/`, `*.sqlite3*`, `__pycache__/`, `*.pyc` to `.gitignore`, and add a `.dockerignore` excluding `.git`, `.env*`, `Docs/`, `.data/`, `tests/`, and `__pycache__/`. |

### Low & Informational Hardening Observations

| ID | Severity | Location | Observation & Remediation |
|---|---|---|---|
| **L-01** | Low | `migrations/001_init_ledgercore_outbox.sql:121-146` | Row-level `BEFORE DELETE` triggers in PostgreSQL do not intercept statement-level `TRUNCATE`. Add a `BEFORE TRUNCATE ON ledger_entries FOR EACH STATEMENT` trigger calling `prevent_ledger_entry_mutation()` and revoke `TRUNCATE` from the application DB role. |
| **L-02** | Low | `src/infrastructure/persistence/sqlite_ledger_repo.py:1139-1155` | `execute_raw_mutation_for_test()` resides on the production repository class. While unexposed via HTTP/services, move raw SQL trigger testing into the test suite (or gate behind an explicit test-only subclass). |
| **L-03** | Low | `src/interfaces/http/server.py:312` | `self.rfile.read(content_length)` has no per-connection socket read timeout (`request.settimeout(10.0)`), leaving the raw stdlib HTTP server susceptible to Slowloris sockets if deployed without a reverse proxy (Nginx/Envoy) or production ASGI/WSGI gateway. |
| **L-04** | Low | `.agents/scripts/safety-gate.py:42-43`, `safety-gate.js:48-49` | Safety Gate fails **open** (`allow`) on malformed JSON exceptions and does not yet match `DROP TABLE`, `DELETE FROM ledger_entries`, or `git push --force-with-lease main`. Switch exception path to fail-closed (`deny`) and expand destructive SQL/git patterns. Also align `.agents/hooks.json` command path (`python .agents/scripts/safety-gate.py`) consistently with hook runner CWD. |
| **L-05** | Low | `src/application/services.py:129,180-185`, `src/domain/exceptions.py:100-103` | (a) `IdempotencyConflictError` returns `expected_fingerprint` in `409` JSON `details` — acceptable for debugging/demo, but remove `expected_fingerprint` in production to prevent offline payload-guessing against predictable keys. (b) Reject requests where `Idempotency-Key` header and body `idempotency_key` are both present with conflicting values, and enforce a max length (e.g., `<= 128` chars) + `MAX_AMOUNT_CENTS` (`<= 2^63 - 1`) in Python backend parity with UI. |
| **L-06** | Low | `src/interfaces/http/server.py:220-223`, `docker-compose.yml:19-50` | Access logging is disabled by default (`LEDGERCORE_ACCESS_LOG=0`); enable structured JSON audit logging in production. In `docker-compose.yml`, restrict local DB/Redis/Kafka port bindings to `127.0.0.1:` (e.g. `127.0.0.1:5441:5432`) and require Redis `requirepass` + Kafka SASL/SSL in production manifests. |
| **L-07** | Info | `Dockerfile:2,15`, `docker-compose.dev.yml:51-53` | Pin `python:3.12-slim` to an immutable SHA-256 digest for supply-chain reproducibility; consider running `ledgercore-distributed-dev` with a non-root UID mapping. |
| **L-08** | Info | `src/interfaces/http/server.py:273-284` | Per RFC 9110 §15.5.6, include `Allow: GET, POST, OPTIONS` header on `405 Method Not Allowed` responses and explicitly handle `HEAD`/`TRACE` (rejecting `TRACE` with `405`). |
| **L-09** | Info | `ui/index.html:7-9` | Google Fonts `<link>` fetches third-party CSS; for strict air-gapped or zero-egress financial deployments, self-host font assets or rely on local system font stacks with CSP `font-src 'self'`. |
| **L-10** | Info | `src/application/services.py:249-255` | `simulate_settlement_failure` chaos flag is exposed in the transfer DTO for interactive Saga compensation demonstration. Gate or disable chaos-injection flags when `LEDGERCORE_ENV=production`. |

---

## 5. Formal Security Certification Sign-Off

Based on static code inspection, AST query verification, secret & ignore-rule validation, Safety Gate testing, and dynamic HTTP/SQLite attack probing:

- **SQL Injection & Append-Only Double-Entry Immutability**: **VERIFIED & CERTIFIED**
- **Exact Integer-Cent Arithmetic, SHA-256 Idempotency & Lock Fencing**: **VERIFIED & CERTIFIED**
- **XSS-Free DOM Rendering & Strict Numeric Parsing**: **VERIFIED & CERTIFIED**
- **HTTP 64 KB Limit, Security Headers, `405` Guards & Zero Stack-Trace Leakage**: **VERIFIED & CERTIFIED**
- **Non-Root Container, Secret Hygiene, Env Isolation & Safety Gate**: **VERIFIED & CERTIFIED**

**Certification Decision**: **`PASS — CERTIFIED FOR INTEGRATION TO dev`** (and eligible for release to `main` upon concurrent `qa-verifier` test-suite certification; production internet-facing deployment remains subject to resolving Pre-Production Gates **M-01** through **M-05**).
