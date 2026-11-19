"""Main entrypoint for LedgerCore Distributed Saga & Transactional Outbox service."""

from src.interfaces.http.server import main

if __name__ == "__main__":
    main()
