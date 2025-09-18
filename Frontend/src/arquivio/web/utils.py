from __future__ import annotations
from datetime import datetime, timezone

def fmt_unknown(value, default="Unknown"):
    return value if (value is not None and str(value).strip() != "") else default

def timeago(dt) -> str:
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except Exception:
            return "just now"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    diff = max(0, int((now - dt).total_seconds()))
    if diff < 60: return f"{diff}s ago"
    if diff < 3600: return f"{diff//60}m ago"
    if diff < 86400: return f"{diff//3600}h ago"
    return f"{diff//86400}d ago"
