#!/usr/bin/env python3
"""The reviewed DB driver version stays identical in package metadata and CI."""

from __future__ import annotations

import tomllib
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
PSYCOPG_PIN = "psycopg[binary]==3.3.4"


class DependencyContractTest(unittest.TestCase):
    def test_psycopg_cutover_pin_is_consistent(self) -> None:
        metadata = tomllib.loads((REPO / "pyproject.toml").read_text())
        self.assertIn(PSYCOPG_PIN, metadata["project"]["dependencies"])
        workflow = (REPO / ".github" / "workflows" / "tests.yml").read_text()
        self.assertIn(f"pip install '{PSYCOPG_PIN}'", workflow)


if __name__ == "__main__":
    unittest.main()
