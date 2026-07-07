"""Window-wipe guard: a bad API day must not overwrite a healthy window file."""
import os

# main.py imports the full pipeline; give it harmless env so import never
# depends on repo secrets.
os.environ.setdefault("ROSTER_URL", "http://example.invalid/roster.csv")

import main  # noqa: E402


def test_small_existing_file_always_writable():
    # Below the floor there's nothing worth protecting (first runs, test data).
    assert main._window_write_is_safe(new_count=0, existing_count=5)
    assert main._window_write_is_safe(new_count=1, existing_count=19)


def test_wipe_blocked():
    assert not main._window_write_is_safe(new_count=0, existing_count=120)
    assert not main._window_write_is_safe(new_count=54, existing_count=110)


def test_normal_variance_allowed():
    assert main._window_write_is_safe(new_count=56, existing_count=110)
    assert main._window_write_is_safe(new_count=110, existing_count=110)
    assert main._window_write_is_safe(new_count=130, existing_count=110)
