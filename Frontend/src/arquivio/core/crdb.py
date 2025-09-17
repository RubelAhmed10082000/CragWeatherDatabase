import re
from sqlalchemy import text

def parse_crdb_semver(version_str: str) -> tuple[int, int, int]:
    m = re.search(r'\bv?(\d+\.\d+\.\d+)\b', version_str)
    if not m:
        raise ValueError(f"Cannot parse CockroachDB version from: {version_str!r}")
    return tuple(map(int, m.group(1).split('.')))

def get_crdb_version_tuple(conn) -> tuple[int, int, int]:
    ver = conn.execute(text("""
        SELECT value FROM crdb_internal.node_build_info WHERE field = 'Version'
    """)).scalar()
    if not ver:
        ver = conn.execute(text("SELECT version()")).scalar()
    return parse_crdb_semver(ver.lstrip('v') if ver and ver.startswith('v') else ver)
