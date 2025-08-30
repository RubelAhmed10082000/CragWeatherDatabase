import argparse
import uuid
from typing import Optional
from itertools import islice
import pandas as pd
from psycopg import sql

# importing functions and variables
from jobs.weather_updater.app.db import (
    get_connection, ensure_schema, run_sql,
    ROCK_TYPES_ALLOWED, STYLES_ALLOWED,
)

UNKNOWNS = {"UNKNOWN", "UNK", "N/A", "NA", "", None}

def _canon_str(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return None if s == "" or s.upper() in UNKNOWNS else s

def _round_or_none(x: Optional[float], nd: int = 6) -> Optional[float]:
        if x is None or pd.isna(x):
            return None
        return round(float(x), nd)

def _uuid5(ns: str, key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"cragcast:{ns}:{key}"))

def _stable_crag_id(crag_name: str | None, county: str | None,
                    latitude: float | None, longitude: float | None) -> str:
    """
    As crag_df.parquet does not have a crag_id, this derives a unique id from both latitude, longitude, name and county
    """
    key = f"{(crag_name or '').lower()}|{(county or '').lower()}|{latitude if latitude is not None else ''}|{longitude if longitude is not None else ''}"
    return _uuid5("crag", key)

def _s(v) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v)


def _route_uuid(route_id: Optional[str], route_name: str | None,
                crag_name: str | None, difficulty_grade: str | None, safety_grade: str | None) -> str:
    if route_id is not None:
        try:
            return str(uuid.UUID(str(route_id)))
        except Exception:
            pass
    key = f"{_s(route_name)}|{_s(crag_name)}|{_s(difficulty_grade)}|{_s(safety_grade)}"
    return _uuid5("route", key)

def _normalize_vocab(val: Optional[str], allowed: list[str] | None) -> Optional[str]:
    """Map UNKNOWN to NULL; the table CHECK will accept NULL or allowed values."""
    s = _canon_str(val)
    if s is None:
        return None
    if not allowed:
        return s
    for a in allowed:
        if s.lower() == a.lower():
            return a
    return "Other" if any(a.lower() == "other" for a in allowed) else None

def _read_parquet_any(path: str) -> pd.DataFrame:
    """
    reads parquet file from GCS blob storage
    """
    if path.startswith("gs://"):
        return pd.read_parquet(path, storage_options={"token": "google_default"})
    return pd.read_parquet(path)

def _chunk(iterable, n):
    it = iter(iterable)
    while True:
        batch = list(islice(it,n))
        if not batch:
            break
        yield batch

def insert_crags(conn, crags_df: pd.DataFrame, batch_size: int = 5000):
    cols = ["crag_id","crag_name","county","latitude","longitude","rocktype","climbing_style"]
    native = crags_df[cols].astype(object).where(pd.notna(crags_df[cols]), None)
    records = [tuple(row) for row in native.to_numpy()]
    sql = """
        INSERT INTO public.dimcrags (crag_id, crag_name, county, latitude, longitude, rocktype, climbing_style)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (crag_id) DO NOTHING
    """
    with conn.cursor() as cur:
        for batch in _chunk(records, batch_size):
            cur.executemany(sql, batch)

def insert_routes(conn, routes_df: pd.DataFrame, batch_size: int = 5000):
    cols = ["route_id","crag_id","route_name","grade","safety_grade"]
    records = [tuple(row[c] for c in cols) for _, row in routes_df[cols].iterrows()]
    sql = """
        INSERT INTO public.dimroutes (route_id, crag_id, route_name, grade, safety_grade)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (route_id) DO NOTHING
    """
    with conn.cursor() as cur:
        for batch in _chunk(records, batch_size):
            cur.executemany(sql, batch)

def parse_args():
    p = argparse.ArgumentParser(description="Seed dimcrags & dimroutes from a (GCS/local) parquet.")
    p.add_argument("parquet_path", help="Local path or gs:// URI to a parquet file (route-level rows)")
    p.add_argument("--dp", type=int, default=6, help="Rounding precision for lat/lon ")
    p.add_argument("--dry-run", action="store_true", help="Parse & show counts but do not write to DB")
    return p.parse_args()


def main():
    args = parse_args()
    

    df = _read_parquet_any(args.parquet_path)

    ren = {
        "route": "route_name",
        "name": "route_name",
        "region": "county",
        "lat": "latitude",
        "lon": "longitude",
        "type": "climbing_style",

    }
    df = df.rename(columns={k: v for k, v in ren.items() if k in df.columns})

    for col in ["route_id", "route_name", "difficulty_grade", "safety_grade",
                "crag_name", "county", "latitude", "longitude",
                "rocktype", "climbing_style"]:
        if col not in df.columns:
            df[col] = None

    df["route_name"] = df["route_name"].apply(_canon_str)
    df["crag_name"] = df["crag_name"].apply(_canon_str)
    df["county"] = df["county"].apply(_canon_str)
    df["rocktype"] = df["rocktype"].apply(lambda v: _normalize_vocab(v, ROCK_TYPES_ALLOWED))
    df["climbing_style"] = df["climbing_style"].apply(lambda v: _normalize_vocab(v, STYLES_ALLOWED))
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    df["difficulty_grade"] = df["difficulty_grade"].apply(_canon_str)
    df["safety_grade"] = df["safety_grade"].apply(_canon_str)


    df["lat_r"] = df["latitude"].apply(lambda x: _round_or_none(x, args.dp))
    df["lon_r"] = df["longitude"].apply(lambda x: _round_or_none(x, args.dp))

    df["crag_id"] = [
        _stable_crag_id(r.crag_name, r.county, r.latitude, r.longitude)
        for r in df.itertuples(index=False)
    ]
    df["route_id_uuid"] = [
        _route_uuid(getattr(r, "route_id"), r.route_name, r.crag_name, r.difficulty_grade, r.safety_grade)
        for r in df.itertuples(index=False)
    ]

    crag_df = (
        df[["crag_id", "crag_name", "county", "lat_r", "lon_r", "rocktype", "climbing_style"]]
        .dropna(subset=["crag_name"])
        .loc[lambda d: d["crag_name"].str.strip().ne("")]
        .drop_duplicates("crag_id")
        .rename(columns={"lat_r": "latitude", "lon_r": "longitude"})
    )

    routes_df = df[["route_id_uuid", "crag_id", "route_name", "difficulty_grade", "safety_grade"]]
    routes_df = routes_df.dropna(subset=["route_name"]).rename(columns={"route_id_uuid": "route_id", "difficulty_grade":"grade"})

    print(f"Found unique crags: {len(crag_df)}; routes: {len(routes_df)}")
    if args.dry_run:
        print("\n=== DRY RUN VALIDATION ===")

        # NULL audit: crags
        print("\n[crag_df] null counts per column:")
        print(crag_df.isnull().sum())

        # NULL audit: routes
        print("\n[routes_df] null counts per column:")
        print(routes_df.isnull().sum())

        # Duplicate check (natural key for crags)
        dups = (
            crag_df
            .groupby(["crag_name", "county", "latitude", "longitude"])
            .size()
            .reset_index(name="count")
            .query("count > 1")
        )
        print(f"\nDuplicate crag rows by natural key: {len(dups)}")
        if not dups.empty:
            print(dups.head())

        # Counts
        print(f"\nCandidate crag_df count: {len(crag_df)}")
        print(f"Candidate routes_df count: {len(routes_df)}")

        # optional: write debug parquet for inspection
        # crag_df.to_parquet(\"test_crags.parquet\")
        # routes_df.to_parquet(\"test_routes.parquet\")

        print("\n=== END DRY RUN VALIDATION ===")
        return
    

    ensure_schema()

    with get_connection() as conn:
        insert_crags(conn, crag_df, batch_size=5000)
        insert_routes(conn, routes_df, batch_size=5000)

    print("Seed complete: dimcrags + dimroutes updated")

if __name__ == "__main__":
    main()