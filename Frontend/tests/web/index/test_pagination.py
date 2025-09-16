from tests.web.index.conftest import collect_all_rows


def test_clamps_negative_page_and_nonint_per_page_to_safe_defaults(client, soup, html_tools):
    r = client.get("/?page=-5&per_page=abc")
    assert r.status_code == 200
    s = soup(r.data)

    cur, total = html_tools.page(s)
    assert cur == 1

    per_page_input = s.select_one('form#searchForm input[name="per_page"]')
    assert per_page_input is not None
    assert per_page_input.get("value") == "25"

    assert len(html_tools.rows(s)) <= int(per_page_input.get("value"))


def test_overflow_clamps_to_last_page_and_renders_last_slice(client, soup, html_tools):
    r = client.get("/?page=999&per_page=3")
    assert r.status_code == 200
    s = soup(r.data)

    cur, total = html_tools.page(s)
    assert cur == total

    all_rows = collect_all_rows(client, soup, html_tools, "/")
    last_page_rows = html_tools.rows(s)
    k = len(last_page_rows)
    assert k > 0

    expected_last_slice = all_rows[-k:]
    assert last_page_rows == expected_last_slice


def test_filter_change_resets_to_page_1_when_not_via_pager(client, soup, html_tools):
    r = client.get("/?page=2&style=Bouldering")
    assert r.status_code == 200
    s = soup(r.data)
    cur, total = html_tools.page(s)
    assert cur == 1


def test_via_pager_preserves_requested_page(client, soup, html_tools):
    r = client.get("/?county=Derbyshire&per_page=2&page=2&via=pager")
    assert r.status_code == 200
    s = soup(r.data)
    cur, total = html_tools.page(s)
    assert (cur, total) == (2, 3)
