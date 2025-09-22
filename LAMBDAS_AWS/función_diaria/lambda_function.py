import os, json, time, urllib.parse, urllib.request
from datetime import datetime, timezone, timedelta
import boto3

# ===== CONFIG =====
API_KEY = os.environ.get("TMDB_API_KEY")
BUCKET = os.environ.get("BUCKET_NAME", "mi-bucket-local")
PREFIX = os.environ.get("S3_PREFIX", "daily_updates/")
REQUEST_SLEEP = float(os.environ.get("REQUEST_SLEEP", "0.25"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "50"))  # límite por bloque
DAYS_BACK = int(os.environ.get("DAYS_BACK", "1"))
LOCAL_MODE = os.environ.get("LOCAL_MODE", "false").lower() == "true"

BASE = "https://api.themoviedb.org/3"

# Cliente S3 (solo si no estamos en local)
s3 = boto3.client("s3", region_name="eu-west-1")


# ===== HTTP GET =====
def http_get(path, params):
    params["api_key"] = API_KEY
    url = f"{BASE}{path}?{urllib.parse.urlencode(params)}"
    print(f" {url}")
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read().decode("utf-8"))


# ===== PUT JSON =====
def put_json(obj, key):
    if LOCAL_MODE:
        os.makedirs("output", exist_ok=True)
        path = os.path.join("output", key.replace("/", "_"))
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        print(f" Guardado local: {path}")
    else:
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(obj, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
        print(f" Guardado en S3: {key}")


# ===== PROCESADOR GENÉRICO =====
def process_changes(kind, endpoint, start_date, end_date, ts, date_part, block=1):
    saved = 0
    page = 1
    total_pages = 0  # valor por defecto para evitar errores

    while page <= MAX_PAGES:
        data = http_get(endpoint, {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "page": page
        })

        total_pages = int(data.get("total_pages", 1))

        if not data.get("results"):
            print(f" No hay resultados en {endpoint}, página {page}")
            break

        key = f"{PREFIX}{kind}/dt={date_part}/{kind}_changes_page_{page}_block{block}_{ts}.json"
        put_json(data, key)
        saved += 1

        if page >= total_pages:
            break
        page += 1
        time.sleep(REQUEST_SLEEP)

    print(f" Guardadas {saved} páginas de {endpoint} (bloque {block})")
    return saved, page, total_pages


# ===== MAIN HANDLER =====
def lambda_handler(event=None, context=None):
    today = datetime.now(timezone.utc).date()
    start_date = today - timedelta(days=DAYS_BACK)
    end_date = today
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    date_part = start_date.strftime("%Y-%m-%d")

    # Movies
    process_changes("movie", "/movie/changes", start_date, end_date, ts, date_part)

    # TV
    process_changes("tv", "/tv/changes", start_date, end_date, ts, date_part)

    # People
    process_changes("people", "/person/changes", start_date, end_date, ts, date_part)

    return {
        "statusCode": 200,
        "body": " Guardadas todas las páginas de movies, tv y people"
    }
