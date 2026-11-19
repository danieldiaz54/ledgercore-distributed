# SPEC-02: Transactional Outbox, Append-Only Double-Entry Ledger & Persistence Isolation

> **Estado**: Activo  
> **Fecha**: 2026-11-18  
> **Autor**: `data-architect`  
> **Alcance**: Esquema relacional PostgreSQL (`migrations/001_init_ledgercore_outbox.sql`), repositorio SQLite local/dev (`src/infrastructure/persistence/sqlite_ledger_repo.py`), seeding determinista y aislamiento de entornos (`dev` vs. `prod`).

---

## 1. Contexto y Objetivos de Persistencia

`ledgercore-distributed` implementa un motor contable distribuido de doble partida (*double-entry accounting*) con orquestación de Sagas y publicación garantizada de eventos mediante el patrón **Transactional Outbox**. En sistemas financieros distribuidos, actualizar la base de datos y publicar un evento en Kafka como dos operaciones separadas introduce el problema clásico de *dual-write* (estados parcialmente confirmados o eventos fantasma ante caídas).

Esta especificación define los invariantes de persistencia obligatorios:

1. **Inmutabilidad Append-Only del Libro Mayor (`ledger_entries`)**: Ningún asiento contable histórico puede modificarse (`UPDATE`) ni eliminarse (`DELETE`). Toda reversión de una Saga fallida se registra exclusivamente mediante nuevos asientos compensatorios de suma cero (`SAGA_COMPENSATION`). Esta regla se impone a nivel de motor de base de datos mediante *triggers* que lanzan `APPEND_ONLY_LEDGER_VIOLATION`.
2. **Atomicidad del Transactional Outbox**: Cada transición de estado financiero (`RESERVE`, `SETTLE`, `COMPENSATE`) ejecuta en **una única transacción ACID**:
   - Validación y actualización de saldos en `accounts` (con control de concurrencia optimista mediante `version = version + 1`).
   - Inserción de pares balanceados (`DEBIT` / `CREDIT`, suma cero) en `ledger_entries`.
   - Inserción o transición de estado en `transfer_sagas`.
   - Registro de clave de idempotencia y huella de solicitud en `idempotency_records` (en la reserva inicial).
   - Inserción de eventos de dominio pendientes (`PENDING`) en `outbox_events` (y registro en `dead_letter_events` en caso de compensación/fallo).
3. **Resiliencia de Mensajería (DLQ y Deduplicación de Consumidores)**: Persistencia durable de eventos fallidos en `dead_letter_events` y registro de consumidores en `processed_consumer_events` para entrega *at-least-once* con procesamiento efectivo *exactly-once*.
4. **Inversión de Dependencias y Aislamiento de Entornos**: El repositorio de infraestructura traduce filas SQL a entidades puras de dominio (`src/domain/`) sin filtrar tipos SQL/ORM al dominio, y separa estrictamente `.env.development` / `docker-compose.dev.yml` de `.env.production`.

---

## 2. Modelo Entidad-Relación

```mermaid
erDiagram
    accounts ||--o{ ledger_entries : "records"
    accounts ||--o{ transfer_sagas : "source_account"
    accounts ||--o{ transfer_sagas : "destination_account"
    transfer_sagas ||--o| idempotency_records : "guarded_by"
    transfer_sagas ||--o{ outbox_events : "emits"
    outbox_events ||--o{ dead_letter_events : "quarantines"

    accounts {
        text account_id PK
        text owner_name
        text currency
        bigint balance_cents
        text status
        bigint version
        timestamptz created_at
        timestamptz updated_at
    }

    transfer_sagas {
        text saga_id PK
        text idempotency_key UK
        text request_fingerprint
        text source_account_id FK
        text destination_account_id FK
        bigint amount_cents
        text currency
        text status
        bigint fencing_token
        text failure_reason
        timestamptz created_at
        timestamptz updated_at
    }

    ledger_entries {
        text entry_id PK
        text transaction_id
        text account_id FK
        text direction
        bigint amount_cents
        text currency
        text entry_type
        timestamptz created_at
    }

    outbox_events {
        text event_id PK
        text aggregate_id FK
        text topic
        text event_type
        jsonb payload_json
        text status
        integer retry_count
        timestamptz created_at
        timestamptz published_at
    }

    idempotency_records {
        text idempotency_key PK
        text request_fingerprint
        text saga_id FK
        timestamptz created_at
    }

    dead_letter_events {
        text dlq_id PK
        text event_id
        text topic
        text reason
        jsonb payload_json
        timestamptz failed_at
    }

    processed_consumer_events {
        text consumer_group PK
        text event_id PK
        text topic
        timestamptz processed_at
    }
```

---

## 3. Reglas de Inmutabilidad Append-Only (`ledger_entries`)

### 3.1 Política Contable
- Cada operación financiera genera exactamente un conjunto balanceado donde `SUM(DEBIT amount_cents) == SUM(CREDIT amount_cents)` dentro del mismo `transaction_id`, validado antes de persistir mediante `DoubleEntryValidator.validate_balanced(...)`.
- Ningún error operativo o rollback de Saga altera registros existentes en `ledger_entries`.
- Al compensar una Saga (`COMPENSATED`), se añaden dos nuevos asientos con `entry_type = 'SAGA_COMPENSATION'` (`DEBIT` a `ACC-ESCROW-CLEARING`, `CREDIT` a la cuenta origen).
- La auditoría global `compute_global_zero_sum_audit()` exige que la suma total histórica de débitos sea idéntica a la suma total histórica de créditos (`total_debits_cents == total_credits_cents`).

### 3.2 Imposición en Base de Datos (Triggers)
- **PostgreSQL (`migrations/001_init_ledgercore_outbox.sql`)**:
  - Función PL/pgSQL `prevent_ledger_entry_mutation()` que ejecuta `RAISE EXCEPTION 'APPEND_ONLY_LEDGER_VIOLATION: Double-entry ledger entries are strictly append-only';`.
  - Asociada mediante `BEFORE UPDATE OR DELETE ON ledger_entries FOR EACH ROW`.
- **SQLite (`SQLiteLedgerRepository`)**:
  - Trigger `trg_ledger_entries_no_update` (`BEFORE UPDATE ON ledger_entries`): `SELECT RAISE(ABORT, 'APPEND_ONLY_LEDGER_VIOLATION: Double-entry ledger entries are strictly append-only');`.
  - Trigger `trg_ledger_entries_no_delete` (`BEFORE DELETE ON ledger_entries`): `SELECT RAISE(ABORT, 'APPEND_ONLY_LEDGER_VIOLATION: Double-entry ledger entries are strictly append-only');`.
  - En el repositorio Python, cualquier `sqlite3.IntegrityError` o `sqlite3.DatabaseError` que contenga `APPEND_ONLY_LEDGER_VIOLATION` se traduce explícitamente a `AppendOnlyLedgerError`.

---

## 4. Transacciones ACID del Transactional Outbox

```mermaid
sequenceDiagram
    participant App as Saga Orchestrator
    participant Repo as LedgerRepository (ACID TX)
    participant DB as accounts + ledger_entries + transfer_sagas + idempotency_records + outbox_events
    participant Relay as Outbox Relay Worker
    participant Broker as Kafka / Dev Event Bus

    App->>Repo: reserve_funds_with_outbox(cmd, saga_id, fencing_token)
    Repo->>DB: BEGIN IMMEDIATE
    Repo->>DB: Verify source, destination, escrow exist & source.balance_cents >= amount
    Repo->>DB: Validate DoubleEntry (DEBIT source, CREDIT escrow)
    Repo->>DB: UPDATE accounts SET balance_cents, version = version + 1
    Repo->>DB: INSERT ledger_entries (2 rows)
    Repo->>DB: INSERT transfer_sagas (state = RESERVED)
    Repo->>DB: INSERT idempotency_records (key, fingerprint, saga_id)
    Repo->>DB: INSERT outbox_events (status = PENDING, topic = ledger.transfers.reserved)
    Repo->>DB: COMMIT
    Repo-->>App: (SagaInstance, OutboxEvent, (debit_entry, credit_entry))

    Relay->>Repo: list_pending_outbox_events()
    Relay->>Broker: publish(event)
    Relay->>Repo: mark_outbox_published(event_id)
```

### 4.1 Operaciones Transaccionales Atómicas

| Operación | Tablas Afectadas en 1 Transacción ACID | Asientos Contables (Suma Cero) | Evento Outbox | DLQ |
| :--- | :--- | :--- | :--- | :--- |
| **`reserve_funds_with_outbox`** | `accounts`, `ledger_entries`, `transfer_sagas`, `idempotency_records`, `outbox_events` | `DEBIT source`, `CREDIT ACC-ESCROW-CLEARING` (`TRANSFER_RESERVE`) | `topic=ledger.transfers.reserved`, `event_type=TransferFundsReserved`, `status=PENDING` | N/A |
| **`settle_saga_with_outbox`** | `accounts`, `ledger_entries`, `transfer_sagas`, `outbox_events` | `DEBIT ACC-ESCROW-CLEARING`, `CREDIT destination` (`TRANSFER_SETTLE`) | `topic=ledger.transfers.settled`, `event_type=TransferSettled`, `status=PENDING` | N/A |
| **`compensate_saga_with_outbox`** | `accounts`, `ledger_entries`, `transfer_sagas`, `outbox_events`, `dead_letter_events` | `DEBIT ACC-ESCROW-CLEARING`, `CREDIT source` (`SAGA_COMPENSATION`) | `topic=ledger.transfers.compensated`, `event_type=TransferCompensated`, `status=PENDING` | `topic=ledger.transfers.dlq` con `failure_reason` y payload diagnóstico |

### 4.2 Garantías de Concurrencia e Idempotencia
- **Concurrencia Optimista**: Cada mutación sobre `accounts` incrementa `version = version + 1` y actualiza `updated_at`.
- **Fencing Tokens**: `transfer_sagas.fencing_token` almacena el token monotónico asignado por el coordinador de locks distribuidos para rechazar escrituras de líderes obsoletos (*split-brain*).
- **Idempotencia**: `idempotency_records.idempotency_key` es clave primaria única y vincula el `request_fingerprint` con el `saga_id` resultante.
- **Deduplicación de Consumidores**: `processed_consumer_events` usa clave primaria compuesta `(consumer_group, event_id)` para garantizar que reprocesamientos de red o reinicios de brokers nunca dupliquen efectos secundarios.
- **Thread-Safety Local**: `SQLiteLedgerRepository` utiliza `sqlite3.connect(db_path, check_same_thread=False)` con `isolation_level=None` (control explícito de `BEGIN IMMEDIATE` / `COMMIT` / `ROLLBACK`) protegido por `threading.RLock()` y `PRAGMA foreign_keys = ON`.

---

## 5. Catálogo de Cuentas Semilla (Entorno Local/Dev)

El módulo `src/infrastructure/persistence/seed_accounts.py` provee 5 cuentas deterministas idempotentes en centavos enteros (`int`, evitando errores de punto flotante):

| `account_id` | `owner_name` | `currency` | `balance_cents` | Saldo Legible |
| :--- | :--- | :---: | ---: | ---: |
| `ACC-TREASURY-001` | Core Treasury Reserve | `USD` | `50000000` | `$500,000.00` |
| `ACC-MERCHANT-002` | Acme Marketplace Settlement | `USD` | `12500000` | `$125,000.00` |
| `ACC-CUSTOMER-003` | Enterprise Client Wallet | `USD` | `4850000` | `$48,500.00` |
| `ACC-SETTLEMENT-004` | Clearing Partner Bank | `USD` | `8500000` | `$85,000.00` |
| `ACC-ESCROW-CLEARING` | Saga Escrow Clearing Pool | `USD` | `0` | `$0.00` |

---

## 6. Disciplina de Migraciones y Estrategia de Rollback

- **Migración Inicial (`migrations/001_init_ledgercore_outbox.sql`)**:
  - Diseñada para PostgreSQL 16 (`postgres:16-alpine`), ejecutable automáticamente vía `/docker-entrypoint-initdb.d` en `docker-compose.dev.yml` o mediante herramientas de migración en CI/CD.
  - Utiliza sentencias idempotentes (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, `CREATE OR REPLACE FUNCTION`, registro condicional de triggers).
- **Consideraciones de Rollback (No Destructivo en Producción)**:
  - En producción **nunca** se ejecuta `DROP TABLE` ni `TRUNCATE` sobre `ledger_entries` ni sobre registros históricos de `outbox_events` / `dead_letter_events`.
  - Las evoluciones de esquema posteriores (`002_*.sql`, etc.) deben seguir el patrón **Expand-and-Contract** (añadir columnas nullable o nuevas tablas con backfill verificado antes de retirar columnas deprecadas).
  - Para entornos efímeros de desarrollo local, el reinicio limpio se realiza recreando el volumen local de Docker (`docker compose -f docker-compose.dev.yml down -v`) sin tocar ninguna instancia externa.

---

## 7. Aislamiento Estricto de Entornos (`dev` vs. `prod`)

| Parámetro | Desarrollo (`.env.development.example` / `docker-compose.dev.yml`) | Producción (`.env.production.example`) |
| :--- | :--- | :--- |
| `LEDGERCORE_ENV` | `development` | `production` |
| `LEDGERCORE_PORT` | `8096` | `8096` |
| `LEDGERCORE_SQLITE_PATH` | `./.data/ledgercore_dev.sqlite3` | *(Deshabilitado — usa PostgreSQL gestionado)* |
| `POSTGRES_URL` | `postgresql://ledger_dev:ledger_dev_local@localhost:5441/ledgercore_dev` | `postgresql://__PROD_USER__:__PROD_PASSWORD__@__PROD_POSTGRES_HOST__:5432/ledgercore_prod?sslmode=require` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` (Redpanda/Kafka dev local) | `__PROD_KAFKA_BROKERS__` (SASL/SSL obligatorio) |
| `REDIS_URL` | `redis://localhost:6381/0` | `rediss://:__PROD_REDIS_PASSWORD__@__PROD_REDIS_HOST__:6379/0` |

> **Invariante de Seguridad**: Los archivos `.env.development` y `.env.production` están ignorados por `.gitignore`. Ningún script de prueba, seed o contenedor de `docker-compose.dev.yml` puede leer `.env.production` ni conectarse a hosts fuera de la red local de desarrollo.
