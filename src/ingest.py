#!/usr/bin/env python3
"""Load MBTA travel-time and headway CSVs into raw.*, and GTFS names into analytics.dim_stops.

Local files already under data/csv or data/raw are used as-is. Pass --download to pull
any missing year from the MBTA Blue Book portal (the Download button's CSV/zip endpoint)
or the GTFS zip from cdn.mbta.com. Raw fact tables are append-only: a finished file is
recorded in raw.ingest_log and is not loaded again, and nothing in raw is deleted.

Source layouts differ by year. This script aligns them to the columns in
src/data_schema.sql without dropping rows or filling in benchmarks that were not published.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import shutil
import zipfile
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CSV_DIR = DATA_DIR / "csv"
ZIPS_DIR = DATA_DIR / "zips"
GTFS_DIR = DATA_DIR / "MBTA_GTFS"
SCHEMA_PATH = PROJECT_ROOT / "src" / "data_schema.sql"

GTFS_URL = "https://cdn.mbta.com/MBTA_GTFS.zip"
ITEM_DATA_URL = "https://www.arcgis.com/sharing/rest/content/items/{item_id}/data"
FEATURE_DOWNLOAD_API = (
    "https://mbta-massdot.opendata.arcgis.com/api/download/v1/items/{item_id}/csv"
)

# Portal item ids. Zip collections use the ArcGIS item /data endpoint, which is the
# file behind the Hub Download button. 2020-2023 headways are feature services; their
# Download button resolves through the Hub download API to a CSV.
DATASETS = [
    {"kind": "travel", "year": 2020, "item_id": "eda80a5e178e47e9bba6bdb72589d9d9", "format": "zip", "filename": "Travel_Times_2020.zip", "hints": ["2020"]},
    {"kind": "travel", "year": 2021, "item_id": "4892695c4baf42419cbe19bd4995bc3a", "format": "zip", "filename": "Travel_Times_2021.zip", "hints": ["2021", "_21"]},
    {"kind": "travel", "year": 2022, "item_id": "e48c8cd8cf154d27b23d31777a8f39e9", "format": "zip", "filename": "TravelTimes_2022.zip", "hints": ["2022"]},
    {"kind": "travel", "year": 2023, "item_id": "b08854b18ad942e4bc471c8c20e80792", "format": "zip", "filename": "TravelTimes_2023.zip", "hints": ["2023"]},
    {"kind": "travel", "year": 2024, "item_id": "0b4dc16b8b984836962229865d5b573b", "format": "zip", "filename": "TravelTimes_2024.zip", "hints": ["2024"]},
    {"kind": "travel", "year": 2025, "item_id": "5f71a5c035fc4a4dad1b7fa73ba27ef8", "format": "zip", "filename": "TravelTimes_2025.zip", "hints": ["2025"]},
    {"kind": "travel", "year": 2026, "item_id": "682fd13544ac4c518620019a1cb819b1", "format": "zip", "filename": "TravelTimes_2026.zip", "hints": ["2026"]},
    {"kind": "headway", "year": 2020, "item_id": "05d1f5aee81448b6baf7b1094716331f", "format": "csv", "filename": "MBTA_Rapid_Transit_Headways_2020.csv", "hints": ["2020"]},
    {"kind": "headway", "year": 2021, "item_id": "dc61d2ca669842de81ea285457960202", "format": "csv", "filename": "MBTA_Rapid_Transit_Headways_2021.csv", "hints": ["2021"]},
    {"kind": "headway", "year": 2022, "item_id": "0fa93edf7f224546913b489204b074f5", "format": "csv", "filename": "MBTA_Rapid_Transit_Headways_2022.csv", "hints": ["2022"]},
    {"kind": "headway", "year": 2023, "item_id": "896d043f33ae4915b530df12141c772c", "format": "csv", "filename": "MBTA_Rapid_Transit_Headways_2023.csv", "hints": ["2023"]},
    {"kind": "headway", "year": 2024, "item_id": "ccb2941254944803bbd4e2df58e09906", "format": "zip", "filename": "Headways_2024.zip", "hints": ["2024"]},
    {"kind": "headway", "year": 2025, "item_id": "84c9d171d32945f594fbb4d889153c44", "format": "zip", "filename": "Headways_2025.zip", "hints": ["2025"]},
    {"kind": "headway", "year": 2026, "item_id": "fffd5e8ff7f042deb7834f3badf49e58", "format": "zip", "filename": "Headways_2026.zip", "hints": ["2026"]},
]

RAPID_NETWORKS = {"rapid_transit", "m_rapid_transit", "sl_rapid_transit"}

TRAVEL_COLUMNS = [
    "service_date",
    "route_id",
    "from_stop_id",
    "to_stop_id",
    "dep_dt",
    "arr_dt",
    "travel_time_sec",
    "benchmark_travel_time_sec",
]
HEADWAY_COLUMNS = [
    "service_date",
    "route_id",
    "stop_id",
    "dep_dt",
    "prev_dep_dt",
    "headway_sec",
    "benchmark_headway_sec",
]

READ_CHUNK_ROWS = 100_000
SQL_CHUNK_ROWS = 2_000

logger = logging.getLogger("ingest")


def discover_csvs() -> list[Path]:
    """CSVs under data/raw take precedence over the same filename in data/csv."""
    found: dict[str, Path] = {}
    for root in (RAW_DIR, CSV_DIR):
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.csv")):
            found.setdefault(path.name.lower(), path)
    return sorted(found.values(), key=lambda path: path.as_posix())


def classify_csv(path: Path) -> str | None:
    name = path.name.lower()
    if "headway" in name:
        return "headways"
    if "travel" in name:
        return "travel_times"
    return None


def source_key(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


def profile_chunk(df: pd.DataFrame, path: Path, *, full_file_follows: bool) -> None:
    """Print pandas info() and describe() for the first chunk of a source file."""
    scope = (
        "first chunk; later chunks are loaded without another profile"
        if full_file_follows
        else "rows shown below"
    )
    buffer = io.StringIO()
    df.info(buf=buffer)
    logger.info("Profile %s (%s, %s rows)\n%s", path, scope, len(df), buffer.getvalue())
    logger.info("describe() %s\n%s", path.name, df.describe(include="all").to_string())


def _as_id(series: pd.Series) -> pd.Series:
    text_ids = series.astype("string").str.strip()
    text_ids = text_ids.str.replace(r"\.0$", "", regex=True)
    return text_ids.mask(text_ids.isin(["", "<NA>", "nan", "None", "NaN"]))


def _service_dates(series: pd.Series) -> pd.Series:
    """Calendar service date. ArcGIS exports sometimes store midnight Eastern as 05:00 UTC."""
    raw = series.astype("string").str.strip().str.slice(0, 10).str.replace("/", "-", regex=False)
    parsed = pd.to_datetime(raw, errors="coerce")
    dates = []
    for value in parsed:
        dates.append(None if pd.isna(value) else value.date())
    return pd.Series(dates, index=series.index)


def _from_service_seconds(service_date: pd.Series, seconds: pd.Series) -> pd.Series:
    """Seconds from local midnight of the service date. Values past 86400 are the next calendar morning."""
    base = pd.to_datetime(pd.Series(service_date, index=seconds.index))
    delta = pd.to_timedelta(pd.to_numeric(seconds, errors="coerce"), unit="s")
    return base + delta


def _naive_timestamp(series: pd.Series) -> pd.Series:
    """Keep the published clock time.

    2024+ files stamp ISO datetimes with a Z suffix, but the clock matches
    stop_*_sec counted from local midnight of the service date (a 06:20 departure
    is 06:20, not 01:20 Eastern). Dropping the suffix avoids a five-hour shift.
    """
    parsed = pd.to_datetime(series, utc=True, errors="coerce")
    return parsed.dt.tz_localize(None)


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def normalize_travel_times(df: pd.DataFrame) -> pd.DataFrame:
    service_date = _service_dates(df["service_date"])
    if "from_stop_departure_datetime" in df.columns:
        dep_dt = _naive_timestamp(df["from_stop_departure_datetime"])
        arr_dt = _naive_timestamp(df["to_stop_arrival_datetime"])
    else:
        dep_dt = _from_service_seconds(service_date, df["start_time_sec"])
        arr_dt = _from_service_seconds(service_date, df["end_time_sec"])

    benchmark = (
        _numeric(df["benchmark_travel_time_sec"])
        if "benchmark_travel_time_sec" in df.columns
        else pd.Series(pd.NA, index=df.index, dtype="Float64")
    )
    out = pd.DataFrame(
        {
            "service_date": service_date,
            "route_id": _as_id(df["route_id"]),
            "from_stop_id": _as_id(df["from_stop_id"]),
            "to_stop_id": _as_id(df["to_stop_id"]),
            "dep_dt": dep_dt,
            "arr_dt": arr_dt,
            "travel_time_sec": _numeric(df["travel_time_sec"]),
            "benchmark_travel_time_sec": benchmark,
        }
    )
    return out[TRAVEL_COLUMNS]


def normalize_headways(df: pd.DataFrame) -> pd.DataFrame:
    service_date = _service_dates(df["service_date"])
    if "headway_time_sec" in df.columns:
        headway = _numeric(df["headway_time_sec"])
        dep_dt = _from_service_seconds(service_date, df["end_time_sec"])
        prev_dep_dt = _from_service_seconds(service_date, df["start_time_sec"])
    else:
        # Branch headway matches route_id (Green-B, Green-C, ...). Trunk is the
        # combined Green Line figure and is the only value published for heavy rail.
        branch = _numeric(df["headway_branch_seconds"]) if "headway_branch_seconds" in df.columns else None
        trunk = _numeric(df["headway_trunk_seconds"]) if "headway_trunk_seconds" in df.columns else None
        if branch is None and trunk is None:
            headway = pd.Series(pd.NA, index=df.index, dtype="Float64")
        elif branch is None:
            headway = trunk
        elif trunk is None:
            headway = branch
        else:
            headway = branch.where(branch.notna(), trunk)
        dep_dt = _naive_timestamp(df["stop_departure_datetime"])
        prev_dep_dt = dep_dt - pd.to_timedelta(headway, unit="s")

    benchmark = (
        _numeric(df["benchmark_headway_sec"])
        if "benchmark_headway_sec" in df.columns
        else pd.Series(pd.NA, index=df.index, dtype="Float64")
    )
    out = pd.DataFrame(
        {
            "service_date": service_date,
            "route_id": _as_id(df["route_id"]),
            "stop_id": _as_id(df["stop_id"]),
            "dep_dt": dep_dt,
            "prev_dep_dt": prev_dep_dt,
            "headway_sec": headway,
            "benchmark_headway_sec": benchmark,
        }
    )
    return out[HEADWAY_COLUMNS]


def _csv_matches(path: Path, kind: str, hints: list[str]) -> bool:
    name = path.name.lower()
    if kind not in name:
        return False
    return any(hint.lower() in name for hint in hints)


def csvs_present(dataset: dict) -> bool:
    direct = RAW_DIR / dataset["filename"]
    if dataset["format"] == "csv" and direct.exists():
        return True
    for root in (RAW_DIR, CSV_DIR):
        if not root.exists():
            continue
        for path in root.rglob("*.csv"):
            if _csv_matches(path, dataset["kind"], dataset["hints"]):
                return True
    return False


def _session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = "MBTA-Analysis-ingest/1.0"
    return session


def _stream_download(session: requests.Session, url: str, dest: Path) -> None:
    if dest.exists():
        logger.info("Leaving existing download in place: %s", dest)
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    temporary = dest.with_suffix(dest.suffix + ".partial")
    logger.info("Downloading %s -> %s", url, dest)
    with session.get(url, stream=True, timeout=(30, 600)) as response:
        response.raise_for_status()
        with temporary.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                if chunk:
                    handle.write(chunk)
    temporary.replace(dest)


def _extract_csvs(archive: Path, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as archive_file:
        for info in archive_file.infolist():
            if info.is_dir() or not info.filename.lower().endswith(".csv"):
                continue
            name = Path(info.filename).name
            if not name or name.startswith("."):
                continue
            target = dest_dir / name
            if target.exists():
                logger.info("Leaving existing CSV in place: %s", target)
                continue
            with archive_file.open(info) as source, target.open("wb") as handle:
                shutil.copyfileobj(source, handle)
            logger.info("Extracted %s", target)


def _feature_csv_url(session: requests.Session, item_id: str) -> str:
    response = session.get(
        FEATURE_DOWNLOAD_API.format(item_id=item_id),
        params={"layers": 0, "redirect": "false"},
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()
    result_url = payload.get("resultUrl")
    if not result_url:
        raise RuntimeError(f"Portal did not return a CSV URL for item {item_id}: {payload}")
    return result_url


def materialize_datasets(download: bool) -> None:
    """Use CSVs already on disk. Extract a local zip, or download, only when those CSVs are missing.

    Existing files are never replaced.
    """
    session: requests.Session | None = None
    for dataset in DATASETS:
        if csvs_present(dataset):
            continue
        label = f"{dataset['kind']} {dataset['year']}"
        if dataset["format"] == "zip":
            dest = ZIPS_DIR / dataset["filename"]
            if not dest.exists():
                if not download:
                    logger.info("No CSVs for %s. Place them in data/raw or data/csv, or re-run with --download.", label)
                    continue
                session = session or _session()
                _stream_download(session, ITEM_DATA_URL.format(item_id=dataset["item_id"]), dest)
            if dest.exists():
                logger.info("Extracting %s into %s", dest.name, RAW_DIR / dest.stem)
                _extract_csvs(dest, RAW_DIR / dest.stem)
            continue
        dest = RAW_DIR / dataset["filename"]
        if dest.exists():
            continue
        if not download:
            logger.info("No CSVs for %s. Place them in data/raw or data/csv, or re-run with --download.", label)
            continue
        session = session or _session()
        _stream_download(session, _feature_csv_url(session, dataset["item_id"]), dest)


def ensure_gtfs() -> None:
    stops = GTFS_DIR / "stops.txt"
    routes = GTFS_DIR / "routes.txt"
    if stops.exists() and routes.exists():
        return
    archive = ZIPS_DIR / "MBTA_GTFS.zip"
    session = _session()
    _stream_download(session, GTFS_URL, archive)
    GTFS_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as archive_file:
        for info in archive_file.infolist():
            if info.is_dir():
                continue
            name = Path(info.filename).name
            if not name.endswith(".txt"):
                continue
            target = GTFS_DIR / name
            if target.exists():
                continue
            with archive_file.open(info) as source, target.open("wb") as handle:
                shutil.copyfileobj(source, handle)


def _rapid_routes_by_stop(gtfs_dir: Path) -> pd.Series:
    """Map stop_id to route_id values from routes.txt via trips and stop_times.

    stops.txt and routes.txt do not share a key. stop_times.txt and trips.txt are the
    GTFS link between a platform and the routes that serve it.
    """
    routes = pd.read_csv(gtfs_dir / "routes.txt", dtype=str, usecols=["route_id", "network_id"])
    rapid_ids = set(routes.loc[routes["network_id"].isin(RAPID_NETWORKS), "route_id"])
    trips = pd.read_csv(gtfs_dir / "trips.txt", dtype=str, usecols=["trip_id", "route_id"])
    trips = trips[trips["route_id"].isin(rapid_ids)]
    trip_to_route = dict(zip(trips["trip_id"], trips["route_id"], strict=True))
    rapid_trips = set(trip_to_route)

    pieces = []
    stop_times = gtfs_dir / "stop_times.txt"
    for chunk in pd.read_csv(stop_times, dtype=str, usecols=["trip_id", "stop_id"], chunksize=500_000):
        chunk = chunk[chunk["trip_id"].isin(rapid_trips)]
        if chunk.empty:
            continue
        chunk = chunk.copy()
        chunk["route_id"] = chunk["trip_id"].map(trip_to_route)
        pieces.append(chunk[["stop_id", "route_id"]].drop_duplicates())
    if not pieces:
        return pd.Series(dtype="string")
    pairs = pd.concat(pieces, ignore_index=True).drop_duplicates()
    return pairs.groupby("stop_id")["route_id"].agg(lambda values: ", ".join(sorted(set(values))))


def _merge_route_lists(values: pd.Series) -> str:
    parts: set[str] = set()
    for value in values.dropna():
        parts.update(part.strip() for part in str(value).split(",") if part.strip())
    return ", ".join(sorted(parts))


def build_dim_stops(gtfs_dir: Path = GTFS_DIR) -> pd.DataFrame:
    """Stop names and coordinates from stops.txt, route_id from routes.txt."""
    route_by_stop = _rapid_routes_by_stop(gtfs_dir)
    stops = pd.read_csv(
        gtfs_dir / "stops.txt",
        dtype=str,
        usecols=["stop_id", "stop_name", "stop_lat", "stop_lon", "parent_station", "zone_id"],
    )
    stops["route_id"] = stops["stop_id"].map(route_by_stop)

    children = stops.loc[stops["parent_station"].notna() & stops["route_id"].notna()]
    if not children.empty:
        parent_routes = children.groupby("parent_station")["route_id"].agg(_merge_route_lists)
        missing = stops["route_id"].isna() & stops["stop_id"].isin(parent_routes.index)
        stops.loc[missing, "route_id"] = stops.loc[missing, "stop_id"].map(parent_routes)

    keep = stops["route_id"].notna() | (stops["zone_id"] == "RapidTransit")
    dim = stops.loc[keep, ["stop_id", "stop_name", "route_id", "stop_lat", "stop_lon"]].copy()
    dim = dim.rename(columns={"stop_lat": "latitude", "stop_lon": "longitude"})
    dim["latitude"] = pd.to_numeric(dim["latitude"], errors="coerce")
    dim["longitude"] = pd.to_numeric(dim["longitude"], errors="coerce")
    dim = dim.drop_duplicates(subset=["stop_id"])
    return dim


def get_engine() -> Engine:
    load_dotenv(PROJECT_ROOT / ".env")
    url = os.getenv("DATABASE_URL")
    if not url:
        raise SystemExit(
            "DATABASE_URL is not set. Add it to .env, for example "
            "postgresql+psycopg2://user:password@localhost:5432/mbta"
        )
    return create_engine(url)


def apply_schema(engine: Engine) -> None:
    script = SCHEMA_PATH.read_text()
    statements = [part.strip() for part in script.split(";") if part.strip()]
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))


def _loaded_files(engine: Engine) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT source_file FROM raw.ingest_log"))
        return {row[0] for row in rows}


def _for_sql(df: pd.DataFrame) -> pd.DataFrame:
    """Plain numpy/object dtypes. pandas StringDtype and pd.NA break to_sql inserts."""
    out = df.copy()
    for column in out.columns:
        dtype = out[column].dtype
        if column == "service_date":
            out[column] = pd.to_datetime(out[column])
        elif pd.api.types.is_string_dtype(dtype) and not pd.api.types.is_object_dtype(dtype):
            out[column] = out[column].astype(object)
        elif str(dtype) in {"Float64", "Int64"}:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    return out


def load_fact_file(engine: Engine, path: Path, table: str, already_loaded: set[str]) -> int:
    key = source_key(path)
    if key in already_loaded:
        logger.info("Already loaded, leaving raw.%s untouched: %s", table, key)
        return 0

    normalize = normalize_travel_times if table == "travel_times" else normalize_headways
    total = 0
    with engine.begin() as conn:
        reader = pd.read_csv(path, chunksize=READ_CHUNK_ROWS, encoding="utf-8-sig", low_memory=False)
        for index, chunk in enumerate(reader):
            if index == 0:
                profile_chunk(chunk, path, full_file_follows=True)
            prepared = _for_sql(normalize(chunk))
            prepared.to_sql(
                table,
                conn,
                schema="raw",
                if_exists="append",
                index=False,
                chunksize=SQL_CHUNK_ROWS,
                method="multi",
            )
            total += len(prepared)
            logger.info("raw.%s <- %s: %s rows so far", table, path.name, total)
        conn.execute(
            text(
                """
                INSERT INTO raw.ingest_log (source_file, target_table, row_count)
                VALUES (:source_file, :target_table, :row_count)
                """
            ),
            {"source_file": key, "target_table": table, "row_count": total},
        )
    already_loaded.add(key)
    logger.info("Finished raw.%s <- %s (%s rows)", table, key, total)
    return total


def load_facts(engine: Engine) -> list[Path]:
    failures: list[Path] = []
    already_loaded = _loaded_files(engine)
    for path in discover_csvs():
        table = classify_csv(path)
        if table is None:
            logger.info("Skipping unrecognized CSV %s", path)
            continue
        try:
            load_fact_file(engine, path, table, already_loaded)
        except Exception:
            logger.exception("Failed to load %s; raw rows from this file were rolled back", path)
            failures.append(path)
    return failures


def load_dim_stops(engine: Engine) -> int:
    ensure_gtfs()
    stops = pd.read_csv(GTFS_DIR / "stops.txt")
    routes = pd.read_csv(GTFS_DIR / "routes.txt")
    profile_chunk(stops, GTFS_DIR / "stops.txt", full_file_follows=False)
    profile_chunk(routes, GTFS_DIR / "routes.txt", full_file_follows=False)

    dim = build_dim_stops(GTFS_DIR)
    with engine.begin() as conn:
        # Dimension refresh only. raw.travel_times and raw.headways are not modified.
        conn.execute(text("TRUNCATE TABLE analytics.dim_stops"))
        _for_sql(dim).to_sql(
            "dim_stops",
            conn,
            schema="analytics",
            if_exists="append",
            index=False,
            chunksize=SQL_CHUNK_ROWS,
            method="multi",
        )
    logger.info("Loaded %s rows into analytics.dim_stops", len(dim))
    return len(dim)


def profile_local_files() -> None:
    for path in discover_csvs():
        if classify_csv(path) is None:
            continue
        chunk = next(pd.read_csv(path, chunksize=READ_CHUNK_ROWS, encoding="utf-8-sig", low_memory=False))
        profile_chunk(chunk, path, full_file_follows=False)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Ingest MBTA travel times, headways, and GTFS stop names.")
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download portal CSVs/zips that are not already under data/raw, data/csv, or data/zips.",
    )
    parser.add_argument(
        "--profile-only",
        action="store_true",
        help="Run info()/describe() on local files and do not write to the database.",
    )
    parser.add_argument("--skip-facts", action="store_true", help="Do not load raw travel times or headways.")
    parser.add_argument("--skip-stops", action="store_true", help="Do not refresh analytics.dim_stops.")
    args = parser.parse_args()

    materialize_datasets(download=args.download)

    if args.profile_only:
        profile_local_files()
        if not args.skip_stops and (GTFS_DIR / "stops.txt").exists():
            profile_chunk(pd.read_csv(GTFS_DIR / "stops.txt"), GTFS_DIR / "stops.txt", full_file_follows=False)
            profile_chunk(pd.read_csv(GTFS_DIR / "routes.txt"), GTFS_DIR / "routes.txt", full_file_follows=False)
        return

    engine = get_engine()
    apply_schema(engine)
    failures: list[Path] = []
    if not args.skip_facts:
        failures = load_facts(engine)
    if not args.skip_stops:
        load_dim_stops(engine)
    if failures:
        raise SystemExit(f"{len(failures)} file(s) failed to load: " + ", ".join(str(path) for path in failures))


if __name__ == "__main__":
    main()
