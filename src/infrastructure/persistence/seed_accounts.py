"""Deterministic development seed accounts for ledgercore-distributed."""

from __future__ import annotations

from src.domain.ledger import Account
from src.infrastructure.persistence.sqlite_ledger_repo import SQLiteLedgerRepository

DEFAULT_ACCOUNTS: tuple[Account, ...] = (
    Account(
        account_id="ACC-TREASURY-001",
        owner_name="Core Treasury Reserve",
        currency="USD",
        balance_cents=50_000_000,
    ),
    Account(
        account_id="ACC-MERCHANT-002",
        owner_name="Acme Marketplace Settlement",
        currency="USD",
        balance_cents=12_500_000,
    ),
    Account(
        account_id="ACC-CUSTOMER-003",
        owner_name="Enterprise Client Wallet",
        currency="USD",
        balance_cents=4_850_000,
    ),
    Account(
        account_id="ACC-SETTLEMENT-004",
        owner_name="Clearing Partner Bank",
        currency="USD",
        balance_cents=8_500_000,
    ),
    Account(
        account_id="ACC-ESCROW-CLEARING",
        owner_name="Saga Escrow Clearing Pool",
        currency="USD",
        balance_cents=0,
    ),
)


def seed_default_accounts(repo: SQLiteLedgerRepository) -> list[Account]:
    """Idempotently seed the 5 default development accounts into the repository.

    Existing accounts are preserved so re-running bootstrap does not clobber live
    development balances or optimistic concurrency versions.
    """
    existing_ids = {account.account_id for account in repo.list_accounts()}
    seeded: list[Account] = []
    for account in DEFAULT_ACCOUNTS:
        if account.account_id not in existing_ids:
            seeded.append(repo.upsert_account(account))
        else:
            seeded.append(repo.get_account(account.account_id))
    return seeded
