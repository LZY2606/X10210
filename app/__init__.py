"""Compatibility shim so ``uvicorn app:app`` resolves to the FastAPI app.

The real entrypoint lives in the top-level ``main.py``; importlib is used to
avoid clashing with this package's own name.
"""
import importlib
import sys

if "main" not in sys.modules:
    importlib.import_module("main")
app = sys.modules["main"].app  # noqa: F401
