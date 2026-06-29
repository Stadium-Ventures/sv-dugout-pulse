"""Post to the shared SV automation channel (#sv-automation).

This is the one place any SV product's *automated* output — health checks,
digests, roster alerts, the LLM health monitor — should land. Kent asked
(2026-06-29) that this noise move OFF the product channels (#dugout-pulse,
#player-intel, …) and into a single channel everyone can mute. Live,
human-relevant alerts (HRs, standout recaps, daily performance recaps) stay
on the product channel via the normal SLACK_WEBHOOK_URL.

Posting is via an Incoming Webhook URL in `SV_AUTOMATION_WEBHOOK_URL`. If that
env var isn't set yet (e.g. the channel/webhook hasn't been created), this
logs a warning and prints what it *would* have posted — it never crashes the
caller and never silently falls back to the product channel.

Reusable across repos: copy this file, set SV_AUTOMATION_WEBHOOK_URL to the
same webhook, done.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_WEBHOOK_URL = os.environ.get("SV_AUTOMATION_WEBHOOK_URL", "")


def post_automation(text: str, blocks: Optional[list] = None) -> bool:
    """Post a message to #sv-automation. Returns True on confirmed delivery."""
    if not _WEBHOOK_URL or "YOUR_WEBHOOK" in _WEBHOOK_URL:
        logger.warning(
            "SV_AUTOMATION_WEBHOOK_URL not set — would have posted to "
            "#sv-automation:\n%s",
            text,
        )
        return False

    payload: dict = {"text": text}
    if blocks:
        payload["blocks"] = blocks
    try:
        resp = requests.post(
            _WEBHOOK_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.error(
                "#sv-automation post failed: %s %s", resp.status_code, resp.text
            )
            return False
        logger.info("Posted to #sv-automation: %s", text[:80])
        return True
    except Exception:
        logger.exception("#sv-automation post errored")
        return False
