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
    saved = 0
    for page in range(1, MAX_PAGES + 1):
        data = http_get('/discover/movie', {
            'language': LANG,
            'sort_by': 'popularity.desc',
            'page': page
        })
        key = f"{PREFIX}{page}_page.json"
        put_json(data, key)
        saved += 1
        time.sleep(REQUEST_SLEEP)
    return {'statusCode': 200, 'body': f'Guardadas {saved} páginas en s3://{BUCKET}/{PREFIX}'}

# Prueba local
if __name__ == "__main__":
    print(lambda_handler({}, {}))
