import pandas as pd

EXPECTED_COLS = [
    "date", "precipitation_percentage", "temperature_c",
    "longitude", "latitude", "relative_humidity_percentage",
]


def clean_weather_df_inmem(raw: pd.DataFrame) -> pd.DataFrame:
    """
    In-memory equivalent of clean_weather_data: rename to your DB column names and keep RH in 0–100 Int64. :contentReference[oaicite:5]{index=5}
    """
    if raw is None or raw.empty:
        return pd.DataFrame(columns=EXPECTED_COLS)

    df = raw.rename(columns={
        "temperature_2m": "temperature_c",
        "relative_humidity_2m": "relative_humidity_percentage",
        "precipitation": "precipitation_percentage",
    })
    df["relative_humidity_percentage"] = pd.to_numeric(df["relative_humidity_percentage"], errors="coerce").round().clip(0, 100).astype("Int64")
    return df