from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd
from tqdm import tqdm

RAW_DIR = Path("data/raw")
OUT_PATH = Path("data/interim/tx_2010_2025.parquet")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

KEEP_COLS = [
    "BEGIN_YEARMONTH",
    "BEGIN_DAY",
    "BEGIN_TIME",
    "END_YEARMONTH",
    "END_DAY",
    "END_TIME",
    "EPISODE_ID",
    "EVENT_ID",
    "STATE",
    "CZ_NAME",
    "CZ_TYPE",
    "EVENT_TYPE",
    "MAGNITUDE",
    "INJURIES_DIRECT",
    "INJURIES_INDIRECT",
    "DEATHS_DIRECT",
    "DEATHS_INDIRECT",
    "DAMAGE_PROPERTY",
    "DAMAGE_CROPS",
    "SOURCE",
]

_DAMAGE_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([KMB])?\s*$", re.IGNORECASE)

def parse_damage_to_dollars(value: object) -> float:

    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not pd.isna(value):
        return float(value)

    s = str(value).strip()
    if s == "" or s.upper() == "NA" or s.upper() == "N/A":
        return 0.0

    m = _DAMAGE_RE.match(s)
    if not m:
        return 0.0

    amount = float(m.group(1))
    suffix = (m.group(2) or "").upper()

    mult = 1.0
    if suffix == "K":
        mult = 1_000.0
    elif suffix == "M":
        mult = 1_000_000.0
    elif suffix == "B":
        mult = 1_000_000_000.0

    return amount * mult

def iter_year_files(start_year: int = 2010, end_year: int = 2025) -> Iterable[Path]:
    for year in range(start_year, end_year + 1):
        # Pattern: StormEvents_details-ftp_v1.0_dYYYY_cXXXXXXXX.csv
        matches = sorted(RAW_DIR.glob(f"StormEvents_details-ftp_v1.0_d{year}_c*.csv"))
        if not matches:
            raise FileNotFoundError(f"No CSV found for year {year} in {RAW_DIR}")
        yield matches[-1]

def load_filter_tx_one_file(csv_path: Path, chunksize: int = 200_000) -> pd.DataFrame:
    tx_chunks = []
    usecols = KEEP_COLS

    for chunk in pd.read_csv(
        csv_path,
        usecols=lambda c: c in usecols,
        chunksize=chunksize,
        low_memory=False,
        dtype={
            "STATE": "string",
            "CZ_NAME": "string",
            "CZ_TYPE": "string",
            "EVENT_TYPE": "string",
            "SOURCE": "string",
        },
    ):
        chunk = chunk[chunk["STATE"].str.upper() == "TEXAS"]
        if chunk.empty:
            continue

        # Clean damage
        chunk["DAMAGE_PROPERTY_USD"] = chunk["DAMAGE_PROPERTY"].apply(parse_damage_to_dollars).astype("float64")
        chunk["DAMAGE_CROPS_USD"] = chunk["DAMAGE_CROPS"].apply(parse_damage_to_dollars).astype("float64")
        chunk["TOTAL_DAMAGE_USD"] = (chunk["DAMAGE_PROPERTY_USD"] + chunk["DAMAGE_CROPS_USD"]).astype("float64")

        for col in ["INJURIES_DIRECT", "INJURIES_INDIRECT", "DEATHS_DIRECT", "DEATHS_INDIRECT", "MAGNITUDE"]:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce").fillna(0)

        tx_chunks.append(chunk)

    if not tx_chunks:
        return pd.DataFrame()

    return pd.concat(tx_chunks, ignore_index=True)

def main() -> None:
    all_tx = []
    for f in tqdm(list(iter_year_files()), desc="Processing years"):
        print(f"\n📄 Reading: {f.name}")
        df_tx = load_filter_tx_one_file(f)
        print(f"   -> Texas rows: {len(df_tx):,}")
        if not df_tx.empty:
            all_tx.append(df_tx)

    if not all_tx:
        raise RuntimeError("No Texas records found across all years.")

    df = pd.concat(all_tx, ignore_index=True)

    df["BEGIN_YEAR"] = (df["BEGIN_YEARMONTH"] // 100).astype("int64")
    df["BEGIN_MONTH"] = (df["BEGIN_YEARMONTH"] % 100).astype("int64")

    df.to_parquet(OUT_PATH, index=False)
    print(f"\n✅ Saved Texas dataset: {OUT_PATH}  ({len(df):,} rows)")

if __name__ == "__main__":
    main()
