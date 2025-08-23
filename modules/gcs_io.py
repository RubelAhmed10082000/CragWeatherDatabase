import os, time, fsspec, pandas as pd
from google.cloud import storage
import pandas as pd

BUCKET  = os.getenv("GCS_BUCKET")
if not BUCKET:
    raise RuntimeError("No bucket found")

def gcs_url(*parts):
    """
    Constructs a GCS URL.
    """
    if len(parts) == 1 and isinstance(parts[0], str) and parts[0].startswith("gs://"):
        return parts[0]

    safe_parts = []
    for p in parts:
        if p is None:
            continue
        if not isinstance(p, str):
            raise TypeError(
                f"gcs_url expected strings, got {type(p).__name__}. "
                "Did you accidentally pass a DataFrame instead of a path?"
            )
        safe_parts.append(p.strip("/"))

    if not safe_parts:
        raise ValueError("No valid path parts passed to gcs_url")

    return f"gs://{BUCKET}/" + "/".join(safe_parts)

def read_parquet(gs_uri):
    """
    Reads a parquet file from GCS into a Pandas DataFrame.
    """
    return pd.read_parquet(gs_uri, engine="pyarrow")

def write_parquet(df, gs_uri, **kwargs):
    """
    Writes a Pandas DataFrame to a parquet file in GCS.
    """
    df.to_parquet(gs_uri, index=False)
    kwargs.setdefault("engine", "pyarrow")
    kwargs.setdefault("compression", "snappy")
    kwargs.setdefault("index", False)
    df.to_parquet(gs_uri, **kwargs)

def write_csv(df, gs_uri, *, header=False, na_rep="\\N"):
    """
    Writes a Pandas DataFrame to a CSV file in GCS.
    """
    with fsspec.open(gs_uri, "w", newline="") as f:
        df.to_csv(f, index=False, na_rep=na_rep, header=header)

def atomic_write_parquet(df, final_gs_uri):
    """
    Mimics atomic write by writing to a temp file and then renaming it.
    """
    client = storage.Client()
    bucket_name, *key = final_gs_uri.replace("gs://","").split("/",1)
    if not key:
        raise ValueError("Bad gs:// URI")
    key = key[0]
    bucket = client.bucket(bucket_name)
    tmp_key = f".tmp-{int(time.time())}"
    tmp_uri = f"gs://{bucket_name}/{tmp_key}"
    write_parquet(df, tmp_uri)
    src = bucket.blob(tmp_key)
    dst = bucket.blob(key)
    src.reload()
    bucket.copy_blob(src, bucket, new_name=key)
    src.delete()
    print(f"Wrote {final_gs_uri}")
