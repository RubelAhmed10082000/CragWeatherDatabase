"""CockroachDB data access layer for CragCast.

Defines `CragDatabase`, read-only queries to Cockroach DB database used by api: 
crag search, route listing, and weather reads.
"""

import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any
from arquivio.core.crdb import get_crdb_version_tuple
import pandas as pd
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom exception for database-related errors."""

    pass


class CragDatabase:
    """Read-only accessors for crags, routes, and weather facts.

    Notes:
        - Uses lazy engine creation; first access to `engine` builds the pool.
        - All methods are safe to call repeatedly; connections are short-lived.
        - Time handling is UTC. Weather timestamps returned are ISO-8601 strings.
    """
    T_CRAGS = "dimcrags"
    T_ROUTES = "dimroutes"
    T_FACT = "fact_crag_hourly_weather"
    T_LAST_RAIN = "crag_last_rain_state"

    def __init__(self) -> None:
        self._engine: Engine | None = None

    @property
    def engine(self) -> Engine:
        """Return SQLAlchemy engine.

        Env:
            DATABASE_URL: required DSN for Cockroach (postgres wire).
            SANITY_MODE: if "1", skip connect-on-start sanity check.

        Raises:
            DatabaseError: if the DSN is missing or initial connect fails.
        """
        if self._engine is None:
            raw = os.getenv("DATABASE_URL","") 
            if not raw:
                raise DatabaseError("DATABASE_URL not set")
            url = make_url(raw)
            if url.drivername.startswith(("postgresql", "postgresql+psycopg2")):
                url = url.set(drivername="cockroachdb+psycopg2")
                in_cloud_run = bool(os.getenv("K_SERVICE"))
                if in_cloud_run:
                    host = url.host or ""
                    if host in {"localhost", "127.0.0.1"} or "sslmode=disable" in str(url):
                        raise DatabaseError(f"Refusing to start with invalid prod DSN (host={host})")
            eng = create_engine(url, pool_pre_ping=True, future=True)

            version_tuple = None
            if os.getenv("SANITY_MODE") != "1":
                with eng.connect() as c:
                    c.execute(text("SELECT 1"))
                with eng.connect() as c:
                    version_tuple = get_crdb_version_tuple(c) 
                    if version_tuple[:2] < (23, 1):
                        raise RuntimeError("CockroachDB >= 23.1 required")
            if version_tuple:
                logger.info("Connected to CockroachDB (v%s.%s.%s)", *version_tuple)
            else:
                logger.info("Connected to CockroachDB (version check skipped: SANITY_MODE=1)")
 
            self._engine = eng

        return self._engine

    def close(self) -> None:
        """Close the engine."""
        try:
            if self._engine is not None:
                self._engine.dispose()
                self._engine = None
        except Exception as e:
            logger.warning(f"Error closing database connection: {e}")

    def _df(self, rows: list[dict]) -> pd.DataFrame:
        """Build a DataFrame from a list of dict rows, empty if no rows."""
        return pd.DataFrame(rows) if rows else pd.DataFrame([])

    def get_recent_weather(self, crag_id: str, hours: int = 24) -> pd.DataFrame:
        """Return recent weather for a crag within the last N hours (UTC).

        Args:
            crag_id: UUID string of the crag.
            hours: Window size in hours (default 1).

        Returns:
            Pandas DataFrame with columns: timestamp, temp, humidity, precip, wind.

        Raises:
            DatabaseError: on query/connection failure.
        """
        to_ts = datetime.now(UTC)
        from_ts = to_ts - timedelta(hours=hours)
        sql = text(f"""
            SELECT 
                   date AS timestamp, 
                   ROUND(temperature_c,1) AS temp, 
                   relative_humidity_percentage AS humidity, 
                   precipitation_mm AS precip, 
                   windspeed_ms AS wind
            FROM {self.T_FACT}
            WHERE crag_id = :crag_id
              AND date >= :from_ts AND date <= :to_ts
            ORDER BY date DESC
        """)

        try:
            with self.engine.connect() as c:
                rows = [
                    dict(r._mapping)
                    for r in c.execute(
                        sql, {"crag_id": crag_id, "from_ts": from_ts, "to_ts": to_ts}
                    )
                ]
            df = self._df(rows)
            return df
        except Exception as e:
            logger.error(f"Error fetching weather: {e}")
            raise DatabaseError(f"Weather fetch failed: {e}")

    def get_crag_with_routes(self, crag_id: str) -> Any | None:
        """Return a crag dict with its routes, or None if not found.

        Args:
            crag_id: UUID string.

        Returns:
            Dict with crag fields + `routes: list[dict]`, or None if missing.

        Raises:
            DatabaseError: on query/connection failure.
        """
        crag_sql = text(f"""
        SELECT 
            c.crag_id AS crag_id,
            c.crag_name AS crag_name,
            ''::STRING AS country,
            c.latitude, c.longitude,
            c.county, c.rocktype, c.climbing_style
            FROM {self.T_CRAGS} c
            WHERE c.crag_id = CAST(:crag_id AS UUID)
            """)
        routes_sql = text(f"""
            SELECT
                r.route_id AS route_id,
                r.crag_id,
                r.route_name AS route_name,
                r.grade AS difficulty,
                r.safety_grade AS safety
            FROM {self.T_ROUTES} as r
            WHERE r.crag_id = CAST(:crag_id AS UUID)
            ORDER BY r.route_name
            """)
        try:
            with self.engine.connect() as c:
                crag = c.execute(crag_sql, {"crag_id": crag_id}).mappings().first()
                if not crag:
                    return None
                routes = [
                    dict(r) for r in c.execute(routes_sql, {"crag_id": crag_id}).mappings().all()
                ]
                obj = dict(crag)
                obj["routes"] = routes
                return obj
        except Exception as e:
            logger.error(f"Error fetching crag/routes:{e}")
            raise DatabaseError(f"Crag fetch failed {e}")

    def search_crags(
        self, query: str | None = None, filters: dict[str, list[str]] | None = None
    ) -> pd.DataFrame:
        """Search crags by name/text plus optional multi-select filters.

        Filters use the sentinel value `"__unknown__"` to include NULL/empty values.

        Args:
            query: Free-text query matched against several crag columns.
            filters: Dict of lists for keys: county, rocktype, climbing_style.

        Returns:
            DataFrame of crags with columns: id, name, lat/lon, county, rocktype,
            climbing_style, routes_count, last_rained_ts (ISO), last_rain_severity.

        Raises:
            DatabaseError: on query/connection failure.
        """
        where = ["1=1"]
        params: dict[str, Any] = {}

        if query:
            where.append(
                "(c.crag_name ILIKE '%' || :q || '%' "
                "OR c.county ILIKE '%' || :q || '%' "
                "OR c.rocktype ILIKE '%' || :q || '%' "
                "OR c.climbing_style ILIKE '%' || :q || '%')"
            )
            params["q"] = query
            logging.getLogger("api.db").warning("DB search_crags q=%r", query)

        def _add_in(col: str, values: list[str] | None, key: str):
            """Append IN/unknown conditions for a single filter column.

            `__unknown__` expands to `IS NULL OR ''` in SQL.
            """
            if not values:
                return
            vals = list(values)
            want_unknown = "__unknown__" in vals
            known = [v for v in vals if v != "__unknown__"]
            parts = []
            if known:
                parts.append(f"{col} IN :{key}")
                params[key] = known
            if want_unknown:
                parts.append(f"{col} IS NULL OR {col} = ''")
            if parts:
                where.append("(" + " OR ".join(parts) + ")")

        f = filters or {}
        _add_in("c.county", f.get("county"), "counties")
        _add_in("c.rocktype", f.get("rocktype"), "rocktypes")
        _add_in("c.climbing_style", f.get("climbing_style"), "styles")

        sql = text(f"""
            SELECT
                c.crag_id AS id,
                c.crag_name AS name,
                c.latitude, c.longitude,
                ''::STRING AS country,
                c.county, c.rocktype,
                c.climbing_style as climbing_style,
                COALESCE(rc.routes_count, 0) as routes_count,
                lr.last_rained_ts,
                COALESCE(lr.last_rain_severity, 'light') AS last_rain_severity
            FROM {self.T_CRAGS} c
            LEFT JOIN (
                SELECT crag_id, COUNT(*) AS routes_count
                FROM {self.T_ROUTES}
                GROUP BY crag_id
            ) rc ON rc.crag_id = c.crag_id
            LEFT JOIN {self.T_LAST_RAIN} lr ON lr.crag_id = c.crag_id
            WHERE {" AND ".join(where)}
            ORDER BY c.crag_name
            LIMIT 10000
        """)

        bps = []
        if "counties" in params:
            bps.append(bindparam("counties", expanding=True))
        if "rocktypes" in params:
            bps.append(bindparam("rocktypes", expanding=True))
        if "styles" in params:
            bps.append(bindparam("styles", expanding=True))
        if bps:
            sql = sql.bindparams(*bps)

        try:
            with self.engine.connect() as c:
                rows = [dict(r._mapping) for r in c.execute(sql, params)]
            df = self._df(rows)
            # Normalize last_rained_ts to ISO strings where possible.
            if df is not None and not df.empty and "last_rained_ts" in df.columns:

                def _to_iso(x):
                    if x is None:
                        return None
                    try:
                        return (
                            x.to_pydatetime().isoformat()
                            if hasattr(x, "to_pydatetime")
                            else x.isoformat()
                        )
                    except Exception:
                        s = str(x)
                        return None if s.lower() in ("nat", "none", "null", "nan", "") else s

                df["last_rained_ts"] = df["last_rained_ts"].map(_to_iso)
            return df
        except Exception as e:
            logger.error(f"Error searching crags: {e}")
            raise DatabaseError(f"Crag search failed: {e}")

    def get_forecast(self, crag_id: str, hours: int = 168):
        """Return an hourly forecast window for a crag up to `hours` long.

        The window ends at the latest available fact timestamp and starts
        `hours-1` hours before that (inclusive).

        Args:
            crag_id: UUID string.
            hours: Horizon length (1–168 typical).

        Returns:
            DataFrame with timestamp, temp, humidity, precip, wind.
            Empty DataFrame if no facts exist for `crag_id`.
        """
        latest = self._get_latest_ts(crag_id)
        if not latest:
            return self._df([])
        start = latest - timedelta(hours=hours - 1)

        sql = text(f"""
            SELECT
                date AS timestamp,
                ROUND(temperature_c,1) AS temp,
                relative_humidity_percentage AS humidity,
                precipitation_mm AS precip,
                windspeed_ms AS wind
            FROM {self.T_FACT}
            WHERE crag_id = CAST(:crag_id AS UUID)
                AND date >= :start_ts AND date <= :end_ts
            ORDER BY date ASC
        """)
        with self.engine.connect() as c:
            rows = [
                dict(r._mapping)
                for r in c.execute(sql, {"crag_id": crag_id, "start_ts": start, "end_ts": latest})
            ]
        return self._df(rows)

    def _now_utc_floor_hour(self) -> datetime:
        """Current UTC time floored to the top of the hour."""
        now = datetime.now(UTC)
        return now.replace(minute=0, second=0, microsecond=0)

    def _get_latest_ts(self, crag_id: str):
        """Return the latest weather fact timestamp for the given crag.

        Args:
            crag_id: UUID string.

        Returns:
            A timezone-aware datetime if present; otherwise None.
        """
        sql = text(f"SELECT max(date) AS max_ts FROM {self.T_FACT} WHERE crag_id = :crag_id")
        with self.engine.connect() as c:
            row = c.execute(sql, {"crag_id": crag_id}).mappings().first()
        return row["max_ts"] if row and row["max_ts"] is not None else None

    def get_next_forecast_point(self, crag_id: str, hours: int = 168):
        """Return the first (earliest) point within the current forecast window.

        Args:
            crag_id: UUID string.
            hours: Horizon length to look back from the latest fact.

        Returns:
            Dict with keys: timestamp, temp, humidity, precip, wind; or None.
        """

        latest = self._get_latest_ts(crag_id)
        if not latest:
            return None

        start = latest - timedelta(hours=hours - 1)

        sql = text(f"""
            SELECT
            date AS timestamp,
            temperature_c AS temp,
            relative_humidity_percentage AS humidity,
            precipitation_mm AS precip,
            windspeed_ms AS wind
            FROM {self.T_FACT}
            WHERE crag_id = CAST(:crag_id AS UUID)  
            AND date >= :start_ts 
            AND date <= :end_ts
            ORDER BY date ASC
            LIMIT 1
        """)
        with self.engine.connect() as c:
            row = (
                c.execute(sql, {"crag_id": crag_id, "start_ts": start, "end_ts": latest})
                .mappings()
                .first()
            )
        return dict(row) if row else None

    def get_filter_options(self) -> dict:
        """Return facet lists for filters (unknowns grouped under `__unknown__`)."""
        def _col(c, name: str):
            rows = c.execute(
                text(f"""
                SELECT DISTINCT {name}
                FROM dimcrags
                ORDER BY {name} NULLS FIRST
            """)
            ).fetchall()
            vals = [r[0] for r in rows]

            cleaned, saw_unknown = [], False
            for v in vals:
                if v is None or str(v).strip() == "":
                    saw_unknown = True
                else:
                    cleaned.append(str(v))
            cleaned.sort(key=str)
            if saw_unknown:
                cleaned.insert(0, "__unknown__")
            return cleaned
        
        # Each column is handled independently to keep the queries simple.
        with self.engine.connect() as c:
            counties = _col(c, "county")
            rock_types = _col(c, "rocktype")
            climbing_styles = _col(c, "climbing_style")

        return {
            "countries": [],# Placeholder if I decide to expand countries in the future
            "rock_types": rock_types,
            "counties": counties,
            "climbing_styles": climbing_styles,
        }

    def get_nearest_crag_id(self, lat: float, lon: float) -> int | None:
        """Map a coordinate to the nearest crag_id (squared-distance ordering).

        Args:
            lat: Latitude in decimal degrees.
            lon: Longitude in decimal degrees.

        Returns:
            Integer crag_id if one exists; otherwise None.
        """
        sql = text(f"""
            SELECT crag_id
            FROM {self.T_CRAGS}
            ORDER BY ((latitude - :lat)*(latitude - :lat) + (longitude - :lon)*(longitude - :lon)) ASC
            LIMIT 1
        """)
        with self.engine.connect() as c:
            row = c.execute(sql, {"lat": lat, "lon": lon}).mappings().first()
        return int(row["crag_id"]) if row else None


db = CragDatabase()
