import os, json, time, urllib.parse, urllib.request
from datetime import datetime, timezone, timedelta
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
PREFIX = os.environ.get('S3_PREFIX', 'daily_updates/') # Prefijo en S3 donde se guardan los archivos
REQUEST_SLEEP = float(os.environ.get('REQUEST_SLEEP', '0.25')) # Tiempo de espera entre peticiones a la API
MAX_PAGES = int(os.environ.get('MAX_PAGES', '5'))  # Número máximo de páginas a descargar, 5 para pruebas
DAYS_BACK = int(os.environ.get('DAYS_BACK', '1'))  # Días hacia atrás para la ventana de cambios

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
    # Ventana de cambios: de hoy-DAYS_BACK a hoy (UTC)
    today = datetime.now(timezone.utc).date()
    start_date = today - timedelta(days=DAYS_BACK)
    end_date = today

    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    date_part = start_date.strftime('%Y-%m-%d')
    saved = 0

    page = 1
    while page <= MAX_PAGES:
        data = http_get('/movie/changes', {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'page': page
        })

        # Guardar en S3
        key = f"{PREFIX}dt={date_part}/changes_page_{page}_{ts}.json"
        put_json(data, key)
        saved += 1

        total_pages = int(data.get('total_pages', 1))
        page += 1
        if page > total_pages:
            break
        time.sleep(REQUEST_SLEEP)

    return {
        'statusCode': 200,
        'body': f'Guardadas {saved} páginas de /movie/changes en s3://{BUCKET}/{PREFIX}dt={date_part}/'
    }

# Prueba local
if __name__ == "__main__":
    print(lambda_handler({}, {}))
