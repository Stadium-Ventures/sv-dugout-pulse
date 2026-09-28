from datetime import date

from scripts import monday_email as m


def test_paused_through_first_week_of_april():
    assert m.is_paused(date(2026, 10, 5))
    assert m.is_paused(date(2027, 4, 5))


def test_resumes_second_week_of_april():
    assert not m.is_paused(date(2027, 4, 12))
    assert not m.is_paused(date(2027, 5, 3))


def test_dry_run_and_test_sends_bypass_pause():
    assert not m.is_paused(date(2026, 10, 5), dry_run=True)
    assert not m.is_paused(date(2026, 10, 5), to_override=True)


def test_scheduled_send_exits_without_sending(monkeypatch):
    def boom(_today):
        raise AssertionError("build_payload should not run while paused")
    monkeypatch.setattr(m, "build_payload", boom)
    m.main(["--today", "2026-10-05"])
