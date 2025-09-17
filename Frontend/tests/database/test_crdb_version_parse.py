from arquivio.core.crdb import parse_crdb_semver

def test_handles_cockroach_25():
    s = "CockroachDB CCL v25.2.4 (x86_64-pc-linux-gnu, built 2025/07/31 21:20:50, go1.23.7 X:nocoverageredesign)"
    assert parse_crdb_semver(s) == (25, 2, 4)

def test_handles_plain_v():
    assert parse_crdb_semver("v23.1.12") == (23, 1, 12)

def test_raises_on_garbage():
    import pytest
    with pytest.raises(ValueError):
        parse_crdb_semver("no version here")