"""Test bootstrap: put the repo root on sys.path so `src`, `scripts`, and
`main` import the same way they do in production (repo-root cwd)."""
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
