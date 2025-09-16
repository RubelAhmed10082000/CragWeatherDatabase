from urllib.parse import urlparse

from tests.web.index.conftest import collect_all_rows


def test_apply_filters_shows_only_matching_rows(client, soup, html_tools):
    r = client.get("/?rocktype=Limestone&style=Sport")
    assert r.status_code == 200
    s = soup(r.data)
    names = html_tools.rows(s)
    assert len(names) >= 1
    allowed = {"Raven Tor", "Malham Cove", "Kilnsey Crag"}
    assert all(n in allowed for n in names)


def test_reset_link_returns_to_base_view(client, soup, html_tools):
    filtered = client.get("/?rocktype=Grit")
    sf = soup(filtered.data)
    names_filtered = html_tools.rows(sf)
    assert len(names_filtered) >= 1

    reset = sf.select_one(".filter-actions .reset-btn")
    href = reset["href"] if reset and reset.has_attr("href") else "/"
    assert urlparse(href).path == "/"

    base = client.get(href)
    sb = soup(base.data)
    names_base = html_tools.rows(sb)

    assert len(names_base) >= len(names_filtered)


def test_hidden_inputs_present(client, soup):
    r = client.get("/")
    s = soup(r.data)
    assert s.select_one('input[name="page"]')
    assert s.select_one('input[name="per_page"]')
    assert s.select_one('input[name="sort_by"]')
    assert s.select_one('input[name="sort_order"]')


def test_text_search_q_filters_rows_case_insensitive(client, soup, html_tools):
    r = client.get("/?q=stanage&per_page=50")
    assert r.status_code == 200
    s = soup(r.data)
    assert html_tools.rows(s) == ["Stanage Plantation", "Stanage Popular"]


def test_multi_select_filters_intersection(client, soup, html_tools):
    expected = [
        "Almscliff",
        "Kilnsey Crag",
        "Malham Cove",
        "Raven Tor",
        "Stanage Plantation",
    ]

    seen = []
    r = client.get("/?style=Bouldering&style=Sport")
    assert r.status_code == 200
    s = soup(r.data)
    cur, total = html_tools.page(s)
    seen += html_tools.rows(s)

    for p in range(cur + 1, total + 1):
        r = client.get(f"/?style=Bouldering&style=Sport&page={p}&via=pager")
        assert r.status_code == 200
        s = soup(r.data)
        seen += html_tools.rows(s)

    assert seen == expected


def test_combined_filters_reduce_result_set(client, soup, html_tools):
    rows = collect_all_rows(client, soup, html_tools, "/?county=Derbyshire&rocktype=Grit")
    r = client.get("/?county=Derbyshire&rocktype=Grit&per_page=50")
    assert r.status_code == 200
    assert rows == [
        "Curbar Edge",
        "Froggatt Edge",
        "Stanage Plantation",
        "Stanage Popular",
    ]


def test_sort_by_name_desc(client, soup, html_tools):
    rows = collect_all_rows(client, soup, html_tools, "/?sort_by=name&sort_order=desc")
    expected = [
        "Stanage Popular",
        "Stanage Plantation",
        "Raven Tor",
        "Malham Cove",
        "Kilnsey Crag",
        "Froggatt Edge",
        "Curbar Edge",
        "Almscliff",
    ]
    assert rows == expected


def test_query_with_special_chars_is_handled(client, soup, html_tools):
    q = "Peaks & Dales / Kinder+Edge"
    r = client.get(f"/?q={q}")
    assert r.status_code == 200
    s = soup(r.data)
    assert html_tools.rows(s) == []


def test_sort_by_name_desc(client, soup, html_tools):
    rows = collect_all_rows(client, soup, html_tools, "/?sort_by=name&sort_order=desc")
    expected = [
        "Stanage Popular",
        "Stanage Plantation",
        "Raven Tor",
        "Malham Cove",
        "Kilnsey Crag",
        "Froggatt Edge",
        "Curbar Edge",
        "Almscliff",
    ]
    assert rows == expected
    assert rows == sorted(rows, reverse=True)
