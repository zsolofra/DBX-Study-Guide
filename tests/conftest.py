"""Shared test plumbing.

Tests run against the reference solutions by default. Point them at YOUR code:
    DSA_TARGET=exercises pytest tests -q          (bash)
    $env:DSA_TARGET="exercises"; pytest tests -q  (PowerShell)
In Databricks, the 01_dsa_practice notebook sets this for you via a widget.
"""
import importlib
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Where each module's reference implementation lives.
REFERENCE_PACKAGE = {"scd": "warehouse"}


def load(module: str):
    target = os.environ.get("DSA_TARGET", "reference")
    if target == "exercises":
        return importlib.import_module(f"exercises.{module}")
    return importlib.import_module(f"{REFERENCE_PACKAGE.get(module, 'dsa')}.{module}")


@pytest.fixture
def arrays():
    return load("arrays")


@pytest.fixture
def strings():
    return load("strings")


@pytest.fixture
def bsearch():
    return load("binary_search")


@pytest.fixture
def graphs():
    return load("graphs")


@pytest.fixture
def hashmaps():
    return load("hash_maps")


@pytest.fixture
def scd():
    return load("scd")
