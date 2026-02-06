import re
import gzip
import shutil
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from tqdm import tqdm

BASE_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

DETAILS_PATTERN = re.compile(
    r'(StormEvents_details-ftp_v1\.0_d(?P<year>\d{4})_c(?P<cdate>\d{8})\.csv\.gz)'
)

def fetch_directory_listing() -> str:
    resp = requests.get(BASE_URL, timeout=60)
    resp.raise_for_status()
    return resp.text

def build_latest_file_map(html: str) -> Dict[int, str]:
    """
    Returns {year: filename} mapping, choosing the latest cYYYYMMDD per year.
    """
    matches: List[Tuple[int, int, str]] = []
    for m in DETAILS_PATTERN.finditer(html):
        year = int(m.group("year"))
        cdate = int(m.group("cdate"))
        fname = m.group(1)
        matches.append((year, cdate, fname))

    if not matches:
        raise RuntimeError("No StormEvents_details files found in directory listing. "
                           "NOAA page format may have changed.")

    # Keep only latest cdate per year
    latest: Dict[int, Tuple[int, str]] = {}
    for year, cdate, fname in matches:
        if (year not in latest) or (cdate > latest[year][0]):
            latest[year] = (cdate, fname)

    return {year: fname for year, (cdate, fname) in latest.items()}

def download_file(url: str, out_path: Path) -> None:
    if out_path.exists():
        print(f"✅ Already exists: {out_path.name}")
        return

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        with open(out_path, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True, desc=out_path.name
        ) as pbar:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

def gunzip_file(gz_path: Path, csv_path: Path) -> None:
    if csv_path.exists():
        print(f"✅ Already unzipped: {csv_path.name}")
        return
    with gzip.open(gz_path, "rb") as f_in, open(csv_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    print(f"🧩 Unzipped -> {csv_path.name}")

def main(start_year: int = 2010, end_year: int = 2025) -> None:
    html = fetch_directory_listing()
    latest_map = build_latest_file_map(html)

    missing_years = [y for y in range(start_year, end_year + 1) if y not in latest_map]
    if missing_years:
        print(f"⚠️ No matching details files found for years: {missing_years}")

    for year in range(start_year, end_year + 1):
        if year not in latest_map:
            continue

        gz_name = latest_map[year]
        gz_url = BASE_URL + gz_name
        gz_path = RAW_DIR / gz_name
        csv_path = RAW_DIR / gz_name.replace(".gz", "")

        print(f"\n📥 Year {year}: {gz_name}")
        download_file(gz_url, gz_path)
        gunzip_file(gz_path, csv_path)

if __name__ == "__main__":
    main()
