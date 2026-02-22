# Vercel FastAPI entrypoint (repo root). Exposes backend app so Vercel finds it.
import sys
from pathlib import Path

_backend = Path(__file__).resolve().parent / "backend"
sys.path.insert(0, str(_backend))

from main import app  # backend/main.py
