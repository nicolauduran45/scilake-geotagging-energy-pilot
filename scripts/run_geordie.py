import os
import pandas as pd
from datasets import load_dataset
from tqdm import tqdm
import geordie
import requests
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Config
OUT_PARQUET = "../data/energy_planning_geordie.parquet"
BATCH_SIZE = 20  # how many rows to buffer in memory before flushing
# ---------------------------------------------------------------------------

# 1) Download/load dataset
ds = load_dataset("nicolauduran45/scilake-additional-fulltext-corpus")
df = pd.DataFrame(ds["energy_planning"])

# 2) Create Geordie instance
g = geordie.Geordie(device="cpu")

# ---------------------------------------------------------------------------
# Config
OUT_PARQUET = "../data/energy_planning_geordie.parquet"
# ---------------------------------------------------------------------------

# 1) Download/load dataset
ds = load_dataset("nicolauduran45/scilake-additional-fulltext-corpus")
df = pd.DataFrame(ds["energy_planning"])

# 2) Create Geordie instance
g = geordie.Geordie(device="cpu")

# --- helpers ---------------------------------------------------------------

class TransientProcessError(Exception):
    pass

def safe_process(text):
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None
    try:
        return g.process_text(str(text))
    except requests.exceptions.RequestException as e:
        raise TransientProcessError from e
    except Exception as e:
        msg = str(e)
        if "Nominatim error" in msg or "ReadTimeout" in msg:
            raise TransientProcessError from e
        print(f"⚠️ Non-transient error: {e}")
        return None

def process_sections(sections):
    if not isinstance(sections, (list, tuple)):
        return None
    out = []
    for sec in sections:
        if not isinstance(sec, dict):
            continue
        try:
            content = sec.get("section_content")
            processed = safe_process(content)
        except TransientProcessError:
            return None  # skip this row entirely
        new_sec = dict(sec)
        new_sec["section_content_geordie"] = processed
        out.append(new_sec)
    return out

def process_row(row: pd.Series) -> dict | None:
    row = row.copy()
    try:
        row["title_geordie"] = safe_process(row.get("title"))
        row["abstract_geordie"] = safe_process(row.get("abstract"))
        row["fulltext_sections_geordie"] = process_sections(row.get("fulltext_sections"))
        return row.to_dict()
    except TransientProcessError:
        return None

# --- main loop: collect everything -----------------------------------------

all_rows = []

for idx, r in tqdm(df.iterrows(), total=len(df), desc="GEORDIE"):
    row_out = process_row(r)
    if row_out is None:
        print(f"⏭️ Skipping row {idx} due to transient error.")
        continue
    all_rows.append(row_out)

# --- save once -------------------------------------------------------------

os.makedirs(os.path.dirname(OUT_PARQUET), exist_ok=True)
df_out = pd.DataFrame(all_rows)
df_out.to_parquet(OUT_PARQUET, engine="pyarrow", index=False)

print(f"✅ Finished! Data saved to {OUT_PARQUET} with {len(df_out)} rows")