# jobs/weather_updater/geo/quantize.py
from __future__ import annotations
import math
from typing import Dict, Tuple, List, Callable, Optional
import pandas as pd

Triple = Tuple[str, float, float]                # (crag_id, lat, lon)
CoordsById = Dict[str, Tuple[float, float]]      # crag_id -> (lat, lon)

def _cell_key(lat: float, lon: float, deg: float) -> Tuple[float, float]:
    return (math.floor(lat / deg) * deg, math.floor(lon / deg) * deg)

def group_crags_into_cells(coords_by_id: CoordsById, cell_deg: float) -> Dict[Tuple[float, float], List[Triple]]:
    cells: Dict[Tuple[float, float], List[Triple]] = {}
    for cid, (lat, lon) in coords_by_id.items():
        k = _cell_key(float(lat), float(lon), cell_deg)
        cells.setdefault(k, []).append((cid, float(lat), float(lon)))
    return cells

def quantized_fetch_to_df(
    coords_by_id: CoordsById,
    batch_id: str,
    fetch_fn: Callable[[List[Triple], str, Optional[int]], Optional[pd.DataFrame]],
    *,
    chunk_size: int = 150,
    cell_deg: float = 0.25,
    max_cells: int = 0,  # 0 = all
) -> tuple[pd.DataFrame, int, int]:
    """
    Returns:
      df_all: DataFrame ready for staging (rows for *all* crags via fan-out)
      cells_hit: number of grid cells fetched (API calls ≈ this, divided by chunking)
      crags_covered: total crags represented by those cells
    """
    cells = group_crags_into_cells(coords_by_id, cell_deg)
    cell_keys = list(cells.keys())
    if max_cells and max_cells > 0:
        cell_keys = cell_keys[:max_cells]

    # representative per cell + map to members
    reps: List[Triple] = []
    rep_map: Dict[str, List[Triple]] = {}
    for k in cell_keys:
        members = cells[k]
        rep = members[0]
        reps.append(rep)
        rep_map[rep[0]] = members

    # fetch representatives
    dfs: List[pd.DataFrame] = []
    for i in range(0, len(reps), chunk_size):
        group = reps[i : i + chunk_size]
        df = fetch_fn(group, load_batch_id=batch_id, max_points=None)
        if df is not None and not df.empty:
            dfs.append(df)
    if not dfs:
        return pd.DataFrame(), len(cell_keys), sum(len(cells[k]) for k in cell_keys)

    df_reps = pd.concat(dfs, ignore_index=True)

    # fan-out rows to all members in each cell
    out_parts: List[pd.DataFrame] = []
    for rep_cid, members in rep_map.items():
        rep_rows = df_reps[df_reps["crag_id"] == rep_cid]
        if rep_rows.empty:
            continue
        for (cid, lat, lon) in members:
            if cid == rep_cid:
                out_parts.append(rep_rows)
            else:
                tmp = rep_rows.copy()
                tmp.loc[:, "crag_id"] = cid
                tmp.loc[:, "latitude"] = float(lat)
                tmp.loc[:, "longitude"] = float(lon)
                out_parts.append(tmp)

    if not out_parts:
        return pd.DataFrame(), len(cell_keys), sum(len(cells[k]) for k in cell_keys)
    df_all = pd.concat(out_parts, ignore_index=True)
    return df_all, len(cell_keys), sum(len(cells[k]) for k in cell_keys)
