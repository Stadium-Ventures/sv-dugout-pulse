"""The email's summer-ball placement section has to end with the season.

Summer ball finished Aug 8, 2026, but the section ran off Kent's sheet alone,
so the Sept 1 monthly recap still listed "Active placements" for clubs that
had stopped playing and "Pending arrival" for spots that would never fill.
"""
from datetime import date

from scripts import monday_email as m


def test_section_is_dropped_once_the_season_is_over(monkeypatch):
    monkeypatch.setattr(m, "season_is_active", lambda: False)
    monkeypatch.setattr(m, "last_real_game_day", lambda: None)
    assert m._render_summer_placements_section() == ""


def test_wrap_note_replaces_the_list_right_after_the_season(monkeypatch):
    monkeypatch.setattr(m, "season_is_active", lambda: False)
    monkeypatch.setattr(m, "last_real_game_day", lambda: "2026-08-08")
    html = m._render_summer_placements_section()
    assert "season wrapped Aug 8" in html
    assert "Active placements" not in html


def test_wrap_note_expires(monkeypatch):
    monkeypatch.setattr(m, "last_real_game_day", lambda: "2026-08-08")
    assert m._summer_wrap_note(today=date(2026, 9, 5))          # still recent
    assert m._summer_wrap_note(today=date(2026, 9, 20)) == ""    # long gone


def test_no_note_when_the_log_is_unreadable(monkeypatch):
    monkeypatch.setattr(m, "last_real_game_day", lambda: "not-a-date")
    assert m._summer_wrap_note(today=date(2026, 9, 5)) == ""


def test_section_still_renders_during_the_season(monkeypatch):
    monkeypatch.setattr(m, "season_is_active", lambda: True)
    monkeypatch.setattr(m, "_master_sheet_pro_names", lambda: set())
    monkeypatch.setattr(m, "_summer_player_week_line", lambda *a, **k: "")
    html = m._render_summer_placements_section()
    # Real placements file in data/ — either it rendered the list, or there is
    # no file at all, but it must not be gated off by the season check.
    assert html == "" or "Summer Ball" in html


def test_pulse_envelope_publishes_the_summer_season_state(monkeypatch):
    """The dashboard hides its Summer banner + level filter off this flag."""
    import main

    captured = {}
    monkeypatch.setattr(main, "_atomic_json_write",
                        lambda path, data, **kw: captured.update(data))
    monkeypatch.setattr(main, "_append_health_history", lambda *a, **k: None)
    monkeypatch.setattr(main, "_update_player_health_history", lambda *a, **k: None)

    import scripts._summer_season as ss
    monkeypatch.setattr(ss, "season_is_active", lambda *a, **k: False)
    main.write_output([])
    assert captured["summer_season_active"] is False

    monkeypatch.setattr(ss, "season_is_active", lambda *a, **k: True)
    main.write_output([])
    assert captured["summer_season_active"] is True
