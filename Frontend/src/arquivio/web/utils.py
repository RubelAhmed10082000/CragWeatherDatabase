from __future__ import annotations
from datetime import datetime, timezone

_SENTINELS = {
    "<na>", "na", "n/a", "nan", "none", "null", "", "-", "—"
}


def fmt_unknown(value, default="Unknown"):
    if value is None:
        return default
    s = str(value).strip()
    return default if s.lower() in _SENTINELS else s

def fmt(value, default="—"):
    return fmt_unknown(value, default)

def _parse_dt(dt):
    if isinstance(dt, datetime):
        return dt
    if isinstance(dt, str):
        try:
            return datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except Exception:
            pass
    return None

def timeago(dt) -> str:
    """
    Return 'N unit(s) ago' with full unit names to satisfy tests:
    minute/hour/day/week/month/year.
    """
    d = _parse_dt(dt)
    if not d:
        return "just now"
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    diff = (now - d).total_seconds()

    if diff < 0:
        return "just now"  

    mins  = int(diff // 60)
    hours = int(diff // 3600)
    days  = int(diff // 86400)
    weeks = int(diff // (7 * 86400))
    months = int(diff // (30 * 86400))
    years  = int(diff // (365 * 86400))

    if mins < 1:
        n, unit = 0, "minute"
    elif mins < 60:
        n, unit = mins, "minute"
    elif hours < 24:
        n, unit = hours, "hour"
    elif days < 7:
        n, unit = days, "day"
    elif weeks < 5:
        n, unit = weeks, "week"
    elif months < 12:
        n, unit = months, "month"
    else:
        n, unit = years, "year"

    if n == 1:
        return f"{n} {unit} ago"
    return f"{n} {unit}s ago"
