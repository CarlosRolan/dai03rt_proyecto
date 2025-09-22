# Lambda: fetch & store ACTORS (Parquet a S3)
# Requisitos en layer: requests, pyarrow
# (boto3 viene en el runtime de Lambda)

import os, io, json
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pyarrow as pa
import pyarrow.parquet as pq

# ======= ENV =======
ACCESS_TOKEN  = os.environ["ACCESS_TOKEN"]                # TMDB Bearer
BASE          = os.environ.get("BASE", "https://api.themoviedb.org/3")
BUCKET        = os.environ["BUCKET"]
PREFIX        = os.environ.get("PREFIX", "initial_load")  # raíz en S3
REGION        = os.environ.get("REGION", "eu-west-1")

# Tuning (ajusta a tu gusto)
MAX_WORKERS     = int(os.environ.get("MAX_WORKERS", "12"))     # paralelismo para details
DETAIL_BATCH    = int(os.environ.get("DETAIL_BATCH", "50"))    # ids por oleada
PART_FLUSH_SIZE = int(os.environ.get("PART_FLUSH_SIZE", "5000"))  # cuántos records por fichero aprox.
TIME_GUARD_MS   = int(os.environ.get("TIME_GUARD_MS", "6000")) # margen para cortar y re-invocar

s3 = boto3.client("s3", region_name=REGION)

# -------- HTTP session con backoff ----------
def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}",
    })
    retry = Retry(
        total=6, connect=3, read=3, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"], raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

# -------- Llamada al endpoint de actor --------
def _actor_detail(session: requests.Session, actor_id: int) -> Dict[str, Any]:
    url = f"{BASE}/person/{actor_id}"
    # Añade si quieres: params={"language": "es-ES"}
    r = session.get(url, timeout=30)
    if r.status_code == 404:
        return {"id": actor_id, "_missing": True}
    r.raise_for_status()
    return r.json()

# -------- Guardar Parquet en S3 -------------
def _save_parquet_actors(records: List[Dict[str, Any]], *, part_index: int,
                         s3_prefix: Optional[str] = None, compression: str = "snappy") -> str:
    """
    Escribe un Parquet con nombre: <PREFIX>/actors/{part_index}_actors.parquet
    (o con un prefijo personalizado si pasas s3_prefix).
    """
    if not records:
        return ""
    table = pa.Table.from_pylist(records)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=compression)  # usa "gzip" si tu layer no trae snappy
    buf.seek(0)

    base = s3_prefix.strip("/") if s3_prefix else f"{PREFIX.strip('/')}/actors"
    key = f"{base}/{part_index}_actors.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue(), ContentType="application/octet-stream")
    return key

# -------- Dedupe simple preservando orden ----
def _dedupe_keep_order(seq: List[int]) -> List[int]:
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

# -------- Procesador principal (llámalo desde tu handler) ----
def process_and_store_actors(
    actor_ids: List[int],
    *,
    start_at: int = 0,                 # índice desde el que continuar
    part_index: int = 1,               # número de fichero a partir del cual escribir
    s3_prefix: Optional[str] = None,   # ej: "initial_load/actors/job_1"
    context=None,                      # pasa el 'context' de Lambda si quieres auto-timebox
    return_saved_keys: bool = False    # si True, devuelve las claves S3 escritas
) -> Dict[str, Any]:
    """
    Descarga detalles de actores y guarda en S3 como Parquet en partes.
    - actor_ids: lista (se hace dedupe en este nivel).
    - start_at, part_index: sirven para reintentos/reanudaciones.
    - s3_prefix: sobreescribe el destino (por defecto usa PREFIX/actors).
    - context: si lo pasas, respeta un guardado + retorno antes de agotar tiempo.
    Devuelve:
      {
        "completed": bool,
        "next_start_at": int,        # índice para continuar si no completó
        "next_part_index": int,      # número de fichero siguiente
        "saved_keys": [ .. ]         # opcional
      }
    """
    ids = _dedupe_keep_order([int(x) for x in actor_ids if x is not None])
    total = len(ids)
    if start_at >= total:
        return {"completed": True, "next_start_at": total, "next_part_index": part_index,
                "saved_keys": [] if return_saved_keys else None}

    saved: List[str] = []
    buffer: List[Dict[str, Any]] = []

    def time_left_ms():
        return context.get_remaining_time_in_millis() if context else 1_000_000_000

    with _make_session() as session:
        i = start_at
        while i < total:
            # Corta por tiempo antes de empezar otro batch
            if time_left_ms() < TIME_GUARD_MS:
                if buffer:
                    key = _save_parquet_actors(buffer, part_index=part_index, s3_prefix=s3_prefix)
                    if return_saved_keys and key:
                        saved.append(key)
                    part_index += 1
                    buffer.clear()
                return {
                    "completed": False,
                    "next_start_at": i,
                    "next_part_index": part_index,
                    "saved_keys": saved if return_saved_keys else None
                }

            # Grupo de trabajo (limita nº de futures simultáneos)
            group = ids[i: i + DETAIL_BATCH]
            results: List[Dict[str, Any]] = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futs = {ex.submit(_actor_detail, session, aid): aid for aid in group}
                for fut in as_completed(futs):
                    try:
                        results.append(fut.result())
                    except Exception as e:
                        results.append({"id": futs[fut], "_error": True, "exception": str(e)})

            buffer.extend(results)
            i += len(group)

            # Flush por tamaño o por tiempo
            if len(buffer) >= PART_FLUSH_SIZE or time_left_ms() < TIME_GUARD_MS:
                key = _save_parquet_actors(buffer, part_index=part_index, s3_prefix=s3_prefix)
                if return_saved_keys and key:
                    saved.append(key)
                part_index += 1
                buffer.clear()

        # Fin: guarda remanente
        if buffer:
            key = _save_parquet_actors(buffer, part_index=part_index, s3_prefix=s3_prefix)
            if return_saved_keys and key:
                saved.append(key)
            part_index += 1
            buffer.clear()

    return {
        "completed": True,
        "next_start_at": total,
        "next_part_index": part_index,
        "saved_keys": saved if return_saved_keys else None
    }

# Añade esto al final del archivo (mismo módulo)

lambda_client = boto3.client("lambda", region_name=REGION)

def _read_ids_from_s3(spec: Dict[str, str]) -> list[int]:
    """
    Lee una lista de actor_ids desde un objeto en S3.
    Espera:
      spec = {"bucket": "nombre-bucket", "key": "ruta/al/objeto"}
    Formatos soportados:
      - JSON: [123, 456, ...]
      - Parquet: debe tener al menos una columna con ints
    """
    bucket = spec["bucket"]
    key    = spec["key"]

    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()

    if key.endswith(".json"):
        ids = json.loads(body.decode("utf-8"))
        if not isinstance(ids, list):
            raise ValueError("El JSON no es una lista")
        return [int(x) for x in ids if x is not None]

    elif key.endswith(".parquet"):
        import pyarrow.parquet as pq
        import pyarrow as pa
        table = pq.read_table(io.BytesIO(body))
        if table.num_columns < 1:
            raise ValueError("Parquet vacío")
        # tomar la primera columna como ids
        arr = table.column(0).to_pylist()
        return [int(x) for x in arr if x is not None]

    else:
        raise ValueError(f"Formato de archivo no soportado: {key}")


def lambda_handler(event, context):
    """
    Payload soportado:
    - A) {"actor_ids":[...], ...}  (para listas pequeñas)
    - B) {"ids_s3":{"bucket":"X","key":"path/ids.json"}, ...}  (para listas grandes)
    """
    actor_ids = event.get("actor_ids")
    ids_s3    = event.get("ids_s3")

    if ids_s3 and not actor_ids:
        try:
            actor_ids = _read_ids_from_s3(ids_s3)
        except Exception as e:
            return {"ok": False, "error": f"No pude leer ids desde S3: {e}"}

    if not isinstance(actor_ids, list) or not actor_ids:
        return {"ok": False, "error": "actor_ids vacío o inválido (ni lista ni ids_s3 válido)"}

    start_at   = int(event.get("start_at", 0))
    part_index = int(event.get("part_index", 1))
    s3_prefix  = event.get("s3_prefix")
    ret_keys   = bool(event.get("return_saved_keys", False))

    res = process_and_store_actors(
        actor_ids,
        start_at=start_at,
        part_index=part_index,
        s3_prefix=s3_prefix,
        context=context,
        return_saved_keys=ret_keys,
    )

    # Auto-reanudación: reusar SIEMPRE el puntero S3 si existe (no re-enviar listas grandes)
    if not res.get("completed"):
        next_event = {
            "actor_ids": None if ids_s3 else actor_ids,
            "ids_s3": ids_s3,
            "start_at": res["next_start_at"],
            "part_index": res["next_part_index"],
            "s3_prefix": s3_prefix,
            "return_saved_keys": ret_keys,
        }
        lambda_client.invoke(
            FunctionName=os.environ["AWS_LAMBDA_FUNCTION_NAME"],
            InvocationType="Event",
            Payload=json.dumps(next_event).encode("utf-8"),
        )
        return {"ok": True, "resumed": True, **res}

    return {"ok": True, "resumed": False, **res}

