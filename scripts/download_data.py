"""Download one month of NYC yellow taxi trips and the taxi zone lookup table.

Usage:
    python scripts/download_data.py --month 2024-01
    python scripts/download_data.py --month 2021-01 --source mirror

Sources:
    tlc     Official NYC Taxi & Limousine Commission files (Parquet), any month published by the TLC.
    mirror  DataTalksClub's GitHub mirror (CSV.gz), yellow taxi months 2019-01 to 2021-07.
"""

import argparse
import shutil
import sys
import urllib.request
from pathlib import Path

TLC_TRIPS = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
TLC_ZONES = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
MIRROR_TRIPS = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{month}.csv.gz"
MIRROR_ZONES = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def download(url: str, target: Path) -> None:
    if target.exists():
        print(f"Already downloaded: {target.name}")
        return
    print(f"Downloading {url}")
    tmp = target.with_suffix(target.suffix + ".part")
    with urllib.request.urlopen(url) as response, open(tmp, "wb") as out:
        shutil.copyfileobj(response, out)
    tmp.rename(target)
    print(f"Saved {target} ({target.stat().st_size / 1_000_000:.1f} MB)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", required=True, help="Month to download, as YYYY-MM")
    parser.add_argument("--source", choices=["tlc", "mirror"], default="tlc")
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if args.source == "tlc":
        trips_url, zones_url, suffix = TLC_TRIPS, TLC_ZONES, ".parquet"
    else:
        trips_url, zones_url, suffix = MIRROR_TRIPS, MIRROR_ZONES, ".csv.gz"

    try:
        download(trips_url.format(month=args.month), RAW_DIR / f"yellow_tripdata_{args.month}{suffix}")
        download(zones_url, RAW_DIR / "taxi_zone_lookup.csv")
    except Exception as exc:  # network errors, 404 for an unpublished month
        print(f"Download failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
