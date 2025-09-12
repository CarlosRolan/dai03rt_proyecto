import os
import json
import time
import urllib.request
import boto3
from datetime import datetime


BUCKET = os.environ.get("S3_BUCKET", "mi-bucket")   
LANG = "es"
PREFIX = "initial_load"
REQUEST_SLEEP = 0.3
MAX_PAGES = int(os.environ.get("MAX_PAGES", 0))     # Para que se decraguen todas, 0

try:
    API_KEY = os.environ["TMDB_API_KEY"]
except KeyError:
    raise RuntimeError(" ERROR: No se encontró la variable de entorno TMDB_API_KEY en Lambda")

BASE = "https://api.themoviedb.org/3/"


s3 = boto3.client("s3", region_name="eu-west-1")

def http_get(path):
    sep = "&" if "?" in path else "?"
    url = f"{BASE}{path}{sep}api_key={API_KEY}"
    print(f"📡 Llamando a: {url}")

    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode("utf-8"))
    return data


def put_json(obj, key):
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(obj, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json"
    )
    print(f" Guardado {key} en S3")


def lambda_handler(event, context):
    current_year = datetime.now().year
    year_20_back = current_year - 20
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    saved = 0

    # Para separa por bloques que no se bloquee aws
    start_page = int(event.get("start_page", 1))
    end_page   = int(event.get("end_page", start_page + 49))  #  50 páginas

    # PELICULAS
    page = start_page
    while page <= end_page:
        path = (
            f"movie/popular?language={LANG}&sort_by=popularity.desc"
            f"&primary_release_date.gte={year_20_back}-01-01&page={page}"
        )
        data = http_get(path)

        key = f"{PREFIX}/movies/{page}_movies_{ts}.json"
        put_json(data, key)
        saved += 1
        time.sleep(REQUEST_SLEEP)

        total_pages = data.get("total_pages", 1)
        if page >= total_pages or page >= 500:
            break
        page += 1

    # TV
    page = start_page
    while page <= end_page:
        path = (
            f"discover/tv?language={LANG}&sort_by=popularity.desc"
            f"&first_air_date.gte={year_20_back}-01-01&page={page}"
        )
        data = http_get(path)

        key = f"{PREFIX}/tv/{page}_tv_{ts}.json"
        put_json(data, key)
        saved += 1
        time.sleep(REQUEST_SLEEP)

        total_pages = data.get("total_pages", 1)
        if page >= total_pages or page >= 500:
            break
        page += 1

    # PEOPLE
    page = start_page
    while page <= end_page:
        path = f"person/popular?language={LANG}&page={page}"
        data = http_get(path)

        key = f"{PREFIX}/people/{page}_people_{ts}.json"
        put_json(data, key)
        saved += 1
        time.sleep(REQUEST_SLEEP)

        total_pages = data.get("total_pages", 1)
        if page >= total_pages or page >= 500:
            break
        page += 1

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"✅ Descargadas {saved} páginas en total (movies+tv+people)",
            "start_page": start_page,
            "end_page": page
        })
    }

