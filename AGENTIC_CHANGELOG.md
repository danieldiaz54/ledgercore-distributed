# Agentic Architecture Changelog — ledgercore-distributed

## [2026-11-18] - Release v0.1.0: Double-Entry Saga, Transactional Outbox, Idempotency & Java 21 Blueprint

### Added
- **SDD Specifications (`.specs/`)**:
  - `.specs/01-domain/saga-outbox-double-entry-ledger.spec.md` (`domain-architect`)
  - `.specs/02-data/transactional-outbox-and-immutable-ledger.spec.md` (`data-architect`)
  - `.specs/03-integration/saga-api-outbox-relay-and-java-service.spec.md` (`integration-architect`)
  - `.specs/04-security/security-audit-report.spec.md` (`security-auditor`)
- **Double-Entry Accounting & Saga Core (`src/domain/`)**:
  - Aritmética entera exacta en centavos (`Account`, `LedgerEntry`, `DoubleEntryValidator` Zero-Sum), `TransferCommand` con huella determinística SHA-256, `DistributedLockManager` con *fencing tokens* monotónicos e `InMemoryKafkaBroker` con deduplicación por *consumer group*.
- **Transactional Outbox & Append-Only Ledger (`src/infrastructure/persistence/` & `migrations/`)**:
  - `SQLiteLedgerRepository` con transacciones ACID atómicas (`reserve_funds_with_outbox`, `settle_saga_with_outbox`, `compensate_saga_with_outbox`), triggers `BEFORE UPDATE`/`BEFORE DELETE` (`APPEND_ONLY_LEDGER_VIOLATION`), `seed_accounts.py` y `001_init_ledgercore_outbox.sql`.
- **HTTP API, Java 21 + Spring Boot 3 Blueprint & Minimalist Console (`src/application/`, `src/interfaces/http/`, `java-service/`, `ui/`)**:
  - `LedgerCoreApplicationService`, `server.py` (puerto `8096`, `201`/`200` idempotent replay/`409` conflict/`422` insufficient funds/`405` ledger immutability), blueprint Java 21 + Spring Boot 3.3 en `java-service/`, y consola minimalista en `ui/index.html` con paridad SHA-256 exacta.
- **QA & Security Certification (`tests/`)**:
  - 33/33 pruebas unitarias, estrés concurrente de 24 hilos Zero-Sum, triggers append-only e integración HTTP certificadas por `qa-verifier` y `security-auditor` (`0 Critical, 0 High`).

## [2026-11-18] - Adopción de Estructura Agéntica (Estándar GNV Manager)

### Added
- **`AGENTS.md`**, **`.agents/`**, **`.specs/00-infrastructure/`**, **`.env.*.example`** y **`docker-compose.dev.yml`**.
