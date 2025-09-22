# Requisitos (layer/build): requests, boto3, pandas, pyarrow, urllib3

import os, io, json
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pyarrow as pa
import pyarrow.parquet as pq

# ======= ENV =======
ACCESS_TOKEN = os.environ["ACCESS_TOKEN"]      # TMDB Bearer
BASE         = os.environ["BASE"]              # https://api.themoviedb.org/3
BUCKET       = os.environ["BUCKET"]
PREFIX       = os.environ.get("PREFIX", "initial_load")  # carpeta raíz en S3
REGION       = os.environ.get("REGION", "eu-west-1")

# Tuning
MAX_WORKERS    = int(os.environ.get("MAX_WORKERS", "12"))     # paralelismo para details
DETAIL_BATCH   = int(os.environ.get("DETAIL_BATCH", "50"))    # ids por oleada
TIME_GUARD_MS  = int(os.environ.get("TIME_GUARD_MS", "6000")) # margen para cortar y re-invocar

s3 = boto3.client("s3", region_name=REGION)
lambda_client = boto3.client("lambda", region_name=REGION)

# ------- HTTP session con backoff -------
def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"})
    retry = Retry(
        total=6, connect=3, read=3, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"], raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

# ------- Discover (una página) -------
def _discover_page(session: requests.Session, page: int, gte: date, lte: date) -> Tuple[int, List[int]]:
    url = f"{BASE}/discover/movie"
    params = {
        "language": "es-ES",
        "include_adult": "false",
        "include_video": "false",
        "primary_release_date.gte": gte.strftime("%Y-%m-%d"),
        "primary_release_date.lte": lte.strftime("%Y-%m-%d"),
        "page": page,
    }
    r = session.get(url, params=params, timeout=30); r.raise_for_status()
    d = r.json()
    total_pages = int(d.get("total_pages", 0))
    ids = [m["id"] for m in d.get("results", [])]
    return total_pages, ids

# ------- Details de película -------
def _movie_detail(session: requests.Session, mid: int, language: str | None = None) -> dict:
    url = f"{BASE}/movie/{mid}"
    params = {
        "append_to_response": "credits"
    }
    if language:
        params["language"] = language  # si quieres forzar 'es-ES' o similar
    r = session.get(url, params=params, timeout=30)
    if r.status_code == 404:
        return {"id": mid, "_missing": True}
    r.raise_for_status()
    return r.json()


# ------- Utilidades -------
def _chunks(seq: List[int], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def _save_parquet(records: List[Dict[str, Any]], range_idx: int, ventana_idx: int, part_index: int) -> str:
    """
    Guarda: initial_load/movies/date_range_<range_idx>/ventana_<ventana_idx>/<part_index>_movies.parquet
    """
    if not records:
        return ""
    table = pa.Table.from_pylist(records)
    buf = io.BytesIO()
    # Si tu layer no trae snappy, cambia a "gzip"
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    key = f"{PREFIX.rstrip('/')}/movies/date_range_{range_idx}/ventana_{ventana_idx}/{part_index}_movies.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue(), ContentType="application/octet-stream")
    return key


def _reinvoke_self(payload: Dict[str, Any]) -> None:
    lambda_client.invoke(
        FunctionName=os.environ["AWS_LAMBDA_FUNCTION_NAME"],
        InvocationType="Event",  # async
        Payload=json.dumps(payload).encode("utf-8"),
    )

def _generate_windows(range_from: date, range_to: date, step_days: int):
    """Divide [range_from..range_to] en sub-ventanas de 'step_days' (inclusive)."""
    idx = 1
    cur = range_from
    while cur <= range_to:
        win_to = min(cur + timedelta(days=step_days-1), range_to)
        yield (idx, cur, win_to)
        idx += 1
        cur = win_to + timedelta(days=1)

# ------- Handler -------
def lambda_handler(event, context):
    range_from = date.fromisoformat(event["from"])
    range_to   = date.fromisoformat(event["to"])
    step_days  = int(event.get("step_days", 180))

    # Usa el que venga o deriva uno estable a partir de las fechas
    range_idx = int(event.get("date_range_idx", f"{range_from:%Y%m%d}{range_to:%Y%m%d}"))

    resume = event.get("resume")

    with _make_session() as session:
        if resume:
            _process_window(session, context,
                            range_idx=range_idx,
                            ventana_idx=int(resume["ventana_idx"]),
                            win_from=date.fromisoformat(resume["win_from"]),
                            win_to=date.fromisoformat(resume["win_to"]),
                            next_page=int(resume.get("next_page", 1)),
                            part_index=int(resume.get("part_index", 1)))
            return {"ok": True, "msg": "resumed window processed"}

        for ventana_idx, win_from, win_to in _generate_windows(range_from, range_to, step_days):
            cont = _process_window(session, context,
                                   range_idx=range_idx,
                                   ventana_idx=ventana_idx, win_from=win_from, win_to=win_to,
                                   next_page=1, part_index=1)
            if cont:
                return {"ok": True, "msg": f"timeboxed; auto-resume scheduled for date_range_{range_idx}/ventana_{ventana_idx}"}

    return {"ok": True, "msg": f"all windows in date_range_{range_idx} completed"}



def _process_window(session, context, *, range_idx: int, ventana_idx: int, win_from: date, win_to: date,
                    next_page: int, part_index: int) -> bool:
    details_buffer: List[Dict[str, Any]] = []

    def time_left_ms():
        return context.get_remaining_time_in_millis()

    total_pages, ids = _discover_page(session, next_page, win_from, win_to)
    total_pages = min(total_pages, 500)

    page = next_page
    while page <= total_pages:
        if page != next_page:
            _, ids = _discover_page(session, page, win_from, win_to)

        for group in _chunks(ids, DETAIL_BATCH):
            if time_left_ms() < TIME_GUARD_MS:
                if details_buffer:
                    _save_parquet(details_buffer, range_idx, ventana_idx, part_index)
                    part_index += 1
                    details_buffer.clear()
                _reinvoke_self({
                    "from": win_from.isoformat(), "to": win_to.isoformat(),
                    "step_days": (win_to - win_from).days + 1,
                    "date_range_idx": range_idx,
                    "resume": {
                        "ventana_idx": ventana_idx,
                        "win_from": win_from.isoformat(),
                        "win_to": win_to.isoformat(),
                        "next_page": page,
                        "part_index": part_index
                    }
                })
                return True

            results = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futs = {ex.submit(_movie_detail, session, mid): mid for mid in group}
                for fut in as_completed(futs):
                    try:
                        results.append(fut.result())
                    except Exception as e:
                        results.append({"id": futs[fut], "_error": True, "exception": str(e)})
            details_buffer.extend(results)

        if len(details_buffer) >= 5000 or time_left_ms() < TIME_GUARD_MS:
            _save_parquet(details_buffer, range_idx, ventana_idx, part_index)
            part_index += 1
            details_buffer.clear()
            if time_left_ms() < TIME_GUARD_MS:
                _reinvoke_self({
                    "from": win_from.isoformat(), "to": win_to.isoformat(),
                    "step_days": (win_to - win_from).days + 1,
                    "date_range_idx": range_idx,
                    "resume": {
                        "ventana_idx": ventana_idx,
                        "win_from": win_from.isoformat(),
                        "win_to": win_to.isoformat(),
                        "next_page": page + 1,
                        "part_index": part_index
                    }
                })
                return True

        page += 1

    if details_buffer:
        _save_parquet(details_buffer, range_idx, ventana_idx, part_index)
        details_buffer.clear()
    print(f"[OK] date_range_{range_idx}/ventana_{ventana_idx} {win_from}..{win_to} completada")
    return False
