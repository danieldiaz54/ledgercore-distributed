"""Anti-AI-slop and V2 Institutional Pine Wallet UI contract verification for ledgercore-distributed."""

from __future__ import annotations

import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


class TestLedgerCoreUiV2Contracts(unittest.TestCase):
    def test_institutional_wallet_identity_and_safe_dom(self) -> None:
        html = (REPO_ROOT / "ui" / "index.html").read_text(encoding="utf-8")
        self.assertIn("LedgerCore Distributed", html)
        self.assertIn("Albert Sans", html)
        self.assertIn("#064E3B", html.upper())
        self.assertIn("#F4F7F6", html.upper())
        self.assertNotIn("#F7F6F3", html.upper())
        self.assertNotIn("#8B5CF6", html.upper())
        for sink in ("innerHTML", "outerHTML", "insertAdjacentHTML", "eval("):
            self.assertNotIn(sink, html)
        self.assertTrue((REPO_ROOT / "PRODUCT.md").is_file())
        self.assertTrue((REPO_ROOT / "DESIGN.md").is_file())


if __name__ == "__main__":
    unittest.main()
