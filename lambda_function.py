import os, json, time, urllib.parse, urllib.request
from datetime import datetime, timezone
import boto3

s3 = boto3.client('s3')

# Intentar cargar variables de entorno desde un archivo .env (solo para pruebas locales)
try:
    from dotenv import load_dotenv
    from os.path import dirname, join
    load_dotenv(join(dirname(__file__), ".env"))
except ImportError:
    pass

# Cargar variables de entorno
API_KEY = os.environ['TMDB_API_KEY'] # Clave API de TMDB
BUCKET = os.environ['BUCKET_NAME'] # Nombre del bucket S3
LANG = os.environ.get('TMDB_LANGUAGE', 'es-ES') # Idioma
MAX_PAGES = int(os.environ.get('MAX_PAGES', '2'))  # Número máximo de páginas a descargar, ponemos 2 para pruebas
PREFIX = os.environ.get('S3_PREFIX', 'initial_load/') # Prefijo en S3 donde se guardan los archivos
REQUEST_SLEEP = float(os.environ.get('REQUEST_SLEEP', '0.3')) # Tiempo de espera entre peticiones a la API

BASE = 'https://api.themoviedb.org/3'

# Función para hacer peticiones HTTP GET a la API de TMDB
def http_get(path, params):
    params['api_key'] = API_KEY
    url = f"{BASE}{path}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read().decode('utf-8'))
    
# Función para guardar un objeto JSON en S3
def put_json(obj, key):
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(obj, ensure_ascii=False).encode('utf-8'),
        ContentType='application/json'
    )

# Función principal del Lambda
def lambda_handler(event, context):
    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    year_20_back = datetime.now().year - 20 # Ultimos 20 años
    saved = 0

    # Descargar películas populares
    page = 1
    while True:
        data = http_get('/discover/movie', {
            'language': LANG,
            'sort_by': 'popularity.desc',
            'primary_release_date.gte': f"{year_20_back}-01-01",
            'page': page
        })
        print(data)


        # Guardar en S3 con nombre único
        key = f"{PREFIX}_movies/{page}_movies_{ts}.json"
        put_json(data, key)
        saved += 1
        time.sleep(REQUEST_SLEEP)

        # Número total de páginas que TMDB dice que existen
        total_pages = data.get("total_pages", 1)

        # 🚧 Condiciones de parada
        if MAX_PAGES and page >= MAX_PAGES:  # modo pruebas (ej. MAX_PAGES=2)
            break
        if page >= total_pages:              # modo real (sin límite)
            break

        # Avanzar a la siguiente página
        page += 1




    # Descargar tv
    page = 1
    while True:
        data = http_get('/discover/tv', {
            'language': LANG,
            'sort_by': 'popularity.desc',
            'first_air_date.gte': f"{year_20_back}-01-01",
            'page': page
        })
        print(f"/n{data}")

        # Guardar en S3 con nombre único
        key = f"{PREFIX}_tv/{page}_tv_{ts}.json"
        put_json(data, key)
        saved += 1
        time.sleep(REQUEST_SLEEP)

        # Número total de páginas que TMDB dice que existen
        total_pages = data.get("total_pages", 1)

        # 🚧 Condiciones de parada
        if MAX_PAGES and page >= MAX_PAGES:  # modo pruebas
            break
        if page >= total_pages:              # modo real
            break

        # Avanzar a la siguiente página
        page += 1




     # Descargar personas populares
     
    page = 1
    while True:
        data = http_get('/person/popular', {
            'language': LANG,
            'page': page
        })
        print(f"\n{data}")

        # Guardar en S3 con nombre único
        key = f"{PREFIX}people/{page}_people_{ts}.json"
        put_json(data, key)
        saved += 1
        time.sleep(REQUEST_SLEEP)

        # Número total de páginas que TMDB dice que existen
        total_pages = data.get("total_pages", 1)

        # 🚧 Condiciones de parada
        if MAX_PAGES and page >= MAX_PAGES:  # modo pruebas
            break
        if page >= total_pages:              # modo real
            break

        # Avanzar a la siguiente página
        page += 1
    
     
    return {'statusCode': 200, 'body': f'Guardadas {saved} páginas en s3://{BUCKET}/{PREFIX}'}

     
     
# Prueba local
if __name__ == "__main__":
    print(lambda_handler({}, {}))
