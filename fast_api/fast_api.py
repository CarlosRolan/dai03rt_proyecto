import boto3
from botocore.exceptions import ClientError
import json
from datetime import datetime, timedelta

import requests
import pandas as pd


BUKECT_NAME = "dai03rt-proyecto"

REGION = "eu-west-1"

s3 = boto3.client("s3",aws_access_key_id = ACCESS_KEY_ID,aws_secret_access_key = SECRET_KEY,region_name=REGION) 

lambda_ = boto3.client("lambda",
                  aws_access_key_id = ACCESS_KEY_ID,
                  aws_secret_access_key = SECRET_KEY,
                  region_name=REGION) 

def invocar_lambda(nombre_funcion, payload={}):
    try:
        response = lambda_.invoke(
            FunctionName=nombre_funcion,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload),
        )
        print("Respuesta:")
        result_raw = (response['Payload']).read().decode('utf-8')
        result = json.loads(result_raw)
        print(result)
        return result
    except ClientError as e:
        print(f"Error: {e}")


# FAST_API
# IMPORTS Y CONFIGURACION

import io
import os
from typing import Dict, List
import pandas as pd
import matplotlib.pyplot as plt
from fastapi import Body, FastAPI
from fastapi import Query
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient
from google import genai
import joblib
import numpy as np
import os, sys, json, base64, tempfile, subprocess
from pathlib import Path
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
import json

def get_config_SQL_GRAPH(question, rows_literal, query):
    return f"""Eres un generador determinista de código Python. No ejecutes SQL ni leas archivos. Recibirás:
- SQL (solo contexto)
- ROWS: una lista de diccionarios en sintaxis Python (o JSON equivalente) donde cada diccionario es una fila (vista ya materializada)
- GOAL (la pregunta original del usuario)

Tu tarea: generar SOLO código Python ejecutable que:
1) Importe: pandas (pd), seaborn (sns), matplotlib.pyplot (plt). Evita imports innecesarios.
2) Construya un DataFrame a partir de ROWS insertando el literal EXACTO dentro del código:
   - Define: ROWS = {rows_literal}
   - df = pd.DataFrame(ROWS)
   - No uses eval() ni ast.literal_eval(); no parses cadenas: inserta el literal tal cual.
3) Si el DataFrame está vacío, imprime un mensaje claro y termina sin error.
4) Tipos:
   - Convierte a datetime columnas con nombres como 'release_date','date','created_at','updated_at' con pd.to_datetime(errors='coerce').
   - Convierte a numérico columnas que parezcan numéricas con pd.to_numeric(errors='coerce').
   - Trata como categóricas columnas de texto con baja cardinalidad.
5) Selección de visualización (máx 2 gráficos), **guiada por GOAL + SQL + columnas del DataFrame**. Sigue este orden de decisión y elige la opción más informativa:
   0) **KPI escalar (conteos/totales):** si el resultado es 1×1, o GOAL/SQL indiquen conteo/total (palabras como 'cuántas/os', 'how many', 'count', 'total', o columnas 'count','total','cantidad','num_*'):
      - Dibuja una tarjeta KPI con matplotlib (texto grande centrado, p.ej. fontsize 64) y subtítulo breve con GOAL. Guarda 'chart1.png'. No hagas histogramas triviales.
   a) **Tendencia temporal:** si hay columna fecha o SQL agrupa por fecha y GOAL pide evolución/“a lo largo del tiempo”/“por mes/año”:
      - Agrega por periodo razonable (mes si rango>90 días; si no, día). Dibuja lineplot del conteo y, si existe métrica típica ('popularity','revenue','vote_average','budget','runtime'), también su media (2º gráfico).
   b) **Ranking / Top-N / Bottom-N:** si GOAL pide “top”, “más/menos”, “ranking” sobre una categórica y una numérica:
      - Calcula la métrica (sum/media según SQL o por defecto sum). Ordena desc y muestra **Top-N=20** (o Bottom-N si aplica) con barplot.
   c) **Comparación por categorías:** si GOAL pide “por género/actor/...”: barplot por categórica (Top-20). Si hay una segunda categórica muy relevante, usa hue limitado o facetas moderadas (máximo 6 categorías en hue/facetas).
   d) **Relación entre métricas:** si GOAL sugiere “relación/correlación/X vs Y”, o el SQL selecciona ≥2 numéricas relevantes:
      - Scatterplot (sns.scatterplot o regplot). Muestra sample de máx 1.000 filas si hay muchas.
   e) **Distribución:** si GOAL pide “distribución/variabilidad” o solo hay 1 numérica útil:
      - histplot si la columna tiene ≥15 valores distintos y NO parece un ID. Alternativamente, boxplot.
   f) **Composición / proporciones:** si GOAL habla de “porcentaje/proporción/cuota/desglose” entre dos categóricas o categórica+num:
      - Barplot apilado normalizado (por categoría base) o heatmap de tabla de contingencia (counts o medias).
   g) **1 fila con >1 numéricas:** barplot horizontal de esa única fila, ordenado por valor.
   h) Si ninguna opción produce una visualización informativa, responde EXACTAMENTE 'VIS_IMPOSIBLE'.

6) Buenas prácticas:
   - Títulos y ejes legibles; rota etiquetas si hay muchas categorías.
   - Evita pie charts. Evita histogramas de columnas tipo id/movie_id/actor_id.
   - Limita categorías a Top-20; para scatter/pairplots, sample máx 1.000 filas.
   - Maneja NaN: descarta filas NaN de las columnas usadas para el gráfico.
7) Guarda las figuras como 'chart1.png' y 'chart2.png' (si generas dos). Llama a plt.show() al final.
8) No uses conexiones a bases de datos, variables de entorno ni lecturas/escrituras adicionales (salvo los PNG). No imprimas el SQL completo; puedes incluir un título breve con GOAL y/o una mención corta al contexto.
9) Salida estricta: devuelve SOLO el código Python ejecutable o el texto EXACTO 'VIS_IMPOSIBLE'. No añadas explicación ni markdown.

ENTRADA
SQL:
{query}

ROWS (lista de dicts en sintaxis Python o JSON):
{rows_literal}

GOAL (pregunta original en lenguaje natural):
{question}

SALIDA
- SOLO código Python ejecutable que cumpla las reglas, o el texto exacto 'VIS_IMPOSIBLE'.
"""


def get_config_SQL_to_NL(question, db_view):
    
    return f"""Eres un asistente que transforma una tabla (pandas DataFrame en texto) en una explicación en lenguaje natural.

                    Instrucciones:
                    1) Responde a la PREGUNTA usando exclusivamente los datos de la TABLA proporcionada.
                    2) Si la TABLA está vacía, responde de forma natural indicando que no hay resultados para la consulta.
                    3) Si la PREGUNTA requiere información que no está en las columnas de la TABLA responde EXACTAMENTE:
                    No tengo info suficiente para responder a esa pregunta con la tabla que estoy viendo
                    4) Si en vez de tabla recibes el mensaje "NOT_INFO" responde EXACTAMENTE:
                    La pregunta que me has hecho es imposible de contestar con los datos actuales de la base de datos
                    5) No inventes datos. No incluyas código, SQL ni instrucciones técnicas.
                    6) Sé claro y conciso. Resume tendencias (máximos/mínimos, medias, top-N) solo si ayudan a responder la PREGUNTA; no listes todas las filas salvo que la PREGUNTA lo pida o la TABLA tenga ≤ 10 filas.
                    7) Mantén el idioma de la PREGUNTA original.
                    8) Si mencionas columnas, usa sus nombres exactos.
                    9) Se la input en vez de ser un select es "Error" responde EXACTAMENTE:
                    La lambda "send_query" ha tenido un error de ejecucion y no ha podido generar la tabla para que te responda
                    10) Si en vez de tabla recibes el mensaje "LAMBDA_ERROR":
                    Parece que ha habido un error al intentar consultar nuestra base de datos, por favor intentalo de nuevo más tarde...
                    Disculpa las molestias
                    
                    Entrada:
                    PREGUNTA:
                    {question}

                    TABLA (proporcionada para responder a la pregunta):
                    {db_view}

                    Salida:
                    - Solo un texto en lenguaje natural que responda a la PREGUNTA usando la TABLA, o uno de los mensajes indicados arriba.
                    """
                    
def get_config_NL_to_SQL(pregunta):
    return f"""Eres un generador determinista de SQL. Convierte una pregunta en lenguaje natural en UNA ÚNICA sentencia SQL para la base de datos descrita abajo.

                        Dialecto objetivo: PostgreSQL 15

                        Esquema autorizado (tablas, columnas, tipos, claves). No asumas nada fuera de esto:
                        {DB_SCHEMA}

                        REGLAS OBLIGATORIAS
                        1) SOLO puedes generar sentencias SELECT. Nunca generes INSERT, UPDATE, DELETE, DROP u otras operaciones que modifiquen datos o estructura.
                        2) No inventes tablas, columnas, funciones ni relaciones no presentes en el esquema.
                        3) Si la pregunta requiere datos o campos que NO existen en el esquema, responde EXACTAMENTE:
                        SQL_IMPOSIBLE
                        4) Produce SOLO una sentencia SQL SELECT terminada en punto y coma. Nada de explicaciones, comentarios, CTEs o múltiples sentencias si no son estrictamente necesarias.
                        5) Usa JOINs explícitos y claves tal como figuran en el esquema. Evita SELECT *; nombra columnas relevantes.
                        6) Para filtros de texto tipo “contiene” o búsquedas case-insensitive, usa ILIKE si procede.
                        7) Para fechas relativas, solo si hay columnas de fecha en el esquema:
                        - “últimos 7 días” → WHERE columna_fecha >= CURRENT_DATE - INTERVAL '7 days'
                        - “este año” → date_trunc('year', columna_fecha) = date_trunc('year', CURRENT_DATE)
                        8) Si la pregunta pide top/N, añade ORDER BY apropiado y LIMIT N.
                        9) Si pide agregaciones, usa GROUP BY y agregados correctos.
                        10) Si la pregunta es ambigua pero resoluble con el esquema, elige la interpretación más razonable por nombres/relaciones. Si sigue siendo irresoluble, devuelve el mensaje de insuficiencia.
                        11) Salida estricta: devuelve SOLO la sentencia SQL o el mensaje de insuficiencia, sin texto adicional.
                        12) Ten en cuenta que la pregunta puede pedir información sobre una entidad mal escrita; siempre corrígela y tradúcela al inglés.
                            - Por ejemplo, si piden las películas de un actor pero han escrito mal el nombre, el SELECT no funcionará si usas el nombre tal cual.
                            - Si no tienes la capacidad de saber por quién te están preguntando, di que no conoces a un actor con ese nombre (usa el mensaje de insuficiencia).
                        13) Los géneros, sin embargo, son una lista corta limitada de estas posibilidades:
                        ["Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","History","Horror","Music","Mystery","Romance","Science Fiction","TV Movie","Thriller","War","Western"]
                            - Así que ya sabes qué valores utilizar cuando tengas que hacer alguna consulta que requiera un filtro o agrupación por género.
                        14) Para nombres y géneros con erratas o en español:
                            – No traduzcas nombres propios; genera un filtro robusto con ILIKE a partir de los tokens del nombre (p. ej., “Brada Pitte” → ILIKE '%brad%pitt%').
                            – Mapea términos de género en español/sinónimos a la lista canónica permitida. Ejemplos: “terror/suspenso”→["Horror","Thriller"], “ciencia ficción/sci-fi”→["Science Fiction"], “aventura”→["Adventure"], “familiar”→["Family"], “crimen”→["Crime"], etc.
                            – Si un término no está en el mapeo, responde SQL_IMPOSIBLE.
                            – Recuerda que una película puede tener varios géneros; usa IN (...) sobre g.name.
                        
                        FORMATO DE ENTRADA (usuario)
                        - Texto libre con la pregunta en lenguaje natural (tipo informativo, por ejemplo: "dime las películas más famosas de este año").

                        FORMATO DE SALIDA (obligatorio)
                        -Salida estricta: devuelve solo la sentencia SQL en texto plano, sin bloques de código, sin backticks, sin prefijos como sql. No añadas comentarios ni explicación. Termina en ;.
                        - UNA sentencia SQL SELECT válida para PostgreSQL 15, terminada en ';'
                        O
                        - El texto exacto: SQL_IMPOSIBLE

                        PREGUNTA
                        {pregunta}
                        """

# Contenido de tu archivo .sql como string
DB_SCHEMA = """
-- SCHEMA: tmdb (tablas en 'public')

-- TABLE: movies
CREATE TABLE IF NOT EXISTS public.movies (
    id              INTEGER PRIMARY KEY,                      -- TMDB movie id (manual)
    title           TEXT NOT NULL,
    popularity      REAL,                                     -- float4
    vote_average    NUMERIC(3,1) 
    CHECK (vote_average >= 0 AND vote_average <= 10),     -- 0.0..10.0
    runtime         SMALLINT CHECK (runtime >= 0),            -- minutos
    budget          BIGINT CHECK (budget  >= 0),              -- entero (unidades monetarias)
    revenue         BIGINT CHECK (revenue >= 0),
    overview        TEXT,
    release_date    DATE,
    success         BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_movies_release_date ON public.movies (release_date);
CREATE INDEX IF NOT EXISTS idx_movies_popularity  ON public.movies (popularity);
CREATE INDEX IF NOT EXISTS idx_movies_title_lower ON public.movies (lower(title));

-- TABLE: actors (cast)
CREATE TABLE IF NOT EXISTS public.actors (
    id          INTEGER PRIMARY KEY,                          -- TMDB person id (manual)
    name        TEXT NOT NULL,
    age         SMALLINT CHECK (age BETWEEN 0 AND 150),
    gender      SMALLINT CHECK (gender IN (0,1,2,3)),         -- TMDB: 0 unknown, 1 female, 2 male, 3 non-binary
    popularity  REAL
);

CREATE INDEX IF NOT EXISTS idx_actors_name_lower ON public.actors (lower(name));
CREATE INDEX IF NOT EXISTS idx_actors_popularity ON public.actors (popularity);

-- TABLE: genres
CREATE TABLE IF NOT EXISTS public.genres (
    id      INTEGER PRIMARY KEY,          -- TMDB genre id (manual)
    name    TEXT NOT NULL UNIQUE
);

-- TABLE: movie_actors  (credits → cast, relación N:M)
CREATE TABLE IF NOT EXISTS public.movie_actors (
    movie_id INTEGER NOT NULL REFERENCES public.movies(id) ON DELETE CASCADE,
    actor_id INTEGER NOT NULL REFERENCES public.actors(id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, actor_id)
);

-- TABLE: movie_genres  (genres, relación N:M)
CREATE TABLE IF NOT EXISTS public.movie_genres (
    movie_id INTEGER NOT NULL REFERENCES public.movies(id) ON DELETE CASCADE,
    genre_id INTEGER NOT NULL REFERENCES public.genres(id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, genre_id)
);

CREATE INDEX IF NOT EXISTS idx_movie_actors_actor ON public.movie_actors (actor_id);
CREATE INDEX IF NOT EXISTS idx_movie_genres_genre ON public.movie_genres (genre_id);

"""

API_KEY = "AIzaSyBZrMh8zSQPiBEyM8vNuzpxv6ZXkD42x_Y"
MODEL = "gemini-2.5-flash"

# Configurar Gemini
client = genai.Client(api_key=API_KEY)

# Crear la aplicación FastAPI
app = FastAPI(
    title="Movie Success API",
    description="API para predecir éxito de películas y responder preguntas",
    version="1.0"
)

def _load_model(nombre_del_archivo):
    bundle = joblib.load(nombre_del_archivo)
    scaler = bundle["scaler"]
    clf    = bundle["classifier"]
    return clf, scaler

def _ask_gemini(promt):
    respuesta = client.models.generate_content(model = MODEL,
                                               contents = [{"parts": [{"text": promt}]}])
                                                
    return respuesta.text

@app.post("/predict",
    summary="Predecir éxito de películas",
    description="""
                Envía **una lista de diccionarios** con las features del modelo.
                Las claves deben coincidir con lo que el `scaler` espera.
                """)
def predict(data: List[Dict[str, float]] = Body(
        ...,
        description="Lista de diccionarios con las features del modelo.",
        example=[
            {
                "popularity":0.9957,
                "vote_average":7.1,
                "runtime": 98,
                "budget":9112.5
            },
            {
                "popularity":0.4007,
                "vote_average":5.1,
                "runtime": 0,
                "budget":9112.5
            }
        ]    
    )
):

    try:
        data = pd.DataFrame(data)
        clf, scaler = _load_model("model_and_scaler.joblib")
        X_scaled = scaler.transform(data)
        y_pred  = clf.predict(X_scaled)
        y_proba = clf.predict_proba(X_scaled)
        
        result_body = {
            "y_pred": y_pred,
            "y_proba": y_proba
        }
        
        return {
            "statuscode": 200,
            "result": result_body
        }
        
    except Exception as e:
        error_msg = str(e)
        return {
            "statuscode": 500,
            "result": error_msg
        }
        
@app.get("/ask-text")
def ask_text(question: str = Query(..., min_length=2)):
    
    # Paso 1: humano -> SQL
    
    query = _ask_gemini(get_config_NL_to_SQL(question))

    # Paso 2: tabla simulada
    event = {"QUERY": query}
    response = invocar_lambda("send_query", event)
    
    if response["ok"]:
        table_view = response["result"]
    elif response["ok"]== "NOT_INFO":
        table_view = response["ok"]
    else:
        table_view = "LAMBDA_ERROR"
        

    # Paso 3: tabla -> humano
    
    nl_response = _ask_gemini(get_config_SQL_to_NL(question, table_view))


    return {
        "question": question,
        "query": query,
        "table_view": table_view,
        "nl_repsonse": nl_response
    }

# runner_viz.py


# Lista muy básica de cosas que NO deberían aparecer en el código generado
_BANNED_SNIPPETS = [
    "import os", "import sys", "subprocess", "shutil", "socket", "requests",
    "urllib", "http.server", "pickle", "dill", "cloudpickle",
    "eval(", "exec(", "compile(", "__import__", "input(", "open(", "Path("
]

def execute_viz_code(py_code: str, timeout_sec: int = 20):
    """
    Ejecuta código Python de visualización (seaborn/matplotlib) en un subproceso aislado.
    Espera que el código:
      - construya un DataFrame a partir de ROWS embebido
      - genere una o dos figuras y las guarde como 'chart1.png' y 'chart2.png'
      - haga plt.show() (opcional, no afecta)
    Devuelve: dict con ok, charts(base64), stdout, stderr, returncode.
    """
    code = (py_code or "").strip()
    if code == "VIS_IMPOSIBLE":
        return {"ok": False, "error": "VIS_IMPOSIBLE"}

    # Bloqueo rápido de patrones peligrosos
    lowered = code.lower()
    for bad in _BANNED_SNIPPETS:
        if bad in lowered:
            return {"ok": False, "error": f"Unsafe code rejected: contains '{bad}'"}

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        script_path = td_path / "viz_script.py"

        # Forzamos backend no interactivo antes de ejecutar el código del modelo
        safe_preamble = "import matplotlib; matplotlib.use('Agg')\n"
        script_path.write_text(safe_preamble + code, encoding="utf-8")

        env = os.environ.copy()
        env["MPLBACKEND"] = "Agg"

        try:
            proc = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=td,
                capture_output=True,
                text=True,
                timeout=timeout_sec,
                env=env,
            )
        except subprocess.TimeoutExpired as e:
            return {"ok": False, "error": f"timeout after {timeout_sec}s", "stdout": e.stdout, "stderr": e.stderr}

        charts = []
        for name in ("chart1.png", "chart2.png"):
            p = td_path / name
            if p.exists() and p.is_file():
                charts.append({
                    "name": name,
                    "base64": base64.b64encode(p.read_bytes()).decode("ascii"),
                    "mime": "image/png",
                })

        ok = proc.returncode == 0
        result = {
            "ok": ok,
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "charts": charts,
        }
        if not ok and not charts:
            # Devolvemos algo más explícito si no hay figuras
            result.setdefault("error", "process failed and no charts were produced")
        return result



def _detect_mode(request: Request, override: str | None) -> str:
    """
    Devuelve 'html' o 'code'.
    Prioridad:
      1) override (?format=html|code)
      2) Heurística por User-Agent: si parece navegador -> html; si requests/curl -> code
      3) Fallback: html
    """
    if override in {"html", "code"}:
        return override
    ua = (request.headers.get("user-agent") or "").lower()
    # Navegadores típicos: chrome/safari/firefox/edge -> HTML
    if any(k in ua for k in ["chrome", "safari", "firefox", "edg", "mozilla"]):
        return "html"
    # Clientes CLI/comunes -> CODE
    if "python-requests" in ua or "curl" in ua or "httpie" in ua:
        return "code"
    # Fallback
    return "html"

@app.get("/ask-visual")
def ask_visual(
    request: Request,
    question: str = Query(..., min_length=2),
    format: str | None = Query(None, description="Opcional: forzar 'html' o 'code'")
):
    # -------- 1) NL → SQL --------
    query = _ask_gemini(get_config_NL_to_SQL(question))
    
    if not query:
        raise HTTPException(500, detail="Gemini no devolvió SQL.")
    
    low = query.lower()
    
    if low.startswith("sql_imposible") or low.startswith("no tengo info"):
        # Mismo contrato que tu NL→SQL
        mode = _detect_mode(request, format)
        if mode == "code":
            return PlainTextResponse("VIS_IMPOSIBLE", media_type="text/x-python; charset=utf-8", status_code=422)
        html = f"<!doctype html><meta charset='utf-8'><h1>SQL_IMPOSIBLE</h1><p>No hay info suficiente.</p><pre>{query}</pre>"
        return HTMLResponse(html, status_code=422)
    
    

    # -------- 2) Ejecutar SQL (tu Lambda directa) → ROWS --------
    event = {"QUERY": query}
    response = invocar_lambda("send_query", event)
    
    if response["ok"]:
        table_view = response["result"]
    elif response["ok"]== "NOT_INFO":
        table_view = response["ok"]
    else:
        table_view = "LAMBDA_ERROR"

    # -------- 3) ROWS → código seaborn --------
    code = _ask_gemini(get_config_SQL_GRAPH(question, table_view, query ))
    
    if not code:
        raise HTTPException(500, detail="Gemini no devolvió código.")
    mode = _detect_mode(request, format)

    # -------- 4) Si piden CODE, devolvemos solo el código --------
    if mode == "code":
        # Puede ser 'VIS_IMPOSIBLE' si el modelo no ve viable la visualización
        return PlainTextResponse(code, media_type="text/x-python; charset=utf-8")

    # -------- 5) Si piden HTML, ejecutamos el código y embebemos las imágenes --------
    run = execute_viz_code(code, timeout_sec=20)
    if not run.get("ok"):
        html_err = f"<h1>Error al generar gráficos</h1><pre>{run.get('error') or run.get('stderr') or 'sin detalle'}</pre>"
        return HTMLResponse(html_err, status_code=400)

    imgs = "".join(
        f"""<figure style="max-width:960px">
                <img alt="{c['name']}" src="data:image/png;base64,{c['base64']}" style="width:100%;height:auto"/>
                <figcaption style="font:14px system-ui;color:#555">{c['name']}</figcaption>
            </figure>"""
        for c in run.get("charts", [])
    ) or "<p>No se generaron gráficos.</p>"

    html = f"""<!doctype html><meta charset="utf-8"><title>Viz</title>
<body style="margin:24px;font:16px system-ui">
  <h1 style="margin-top:0">{question}</h1>
  <p style="color:#666"><code>{query}</code></p>
  {imgs}
  <details><summary>STDOUT/STDERR</summary>
    <pre>{(run.get('stdout') or '').strip()}</pre>
    <pre style="color:#a00">{(run.get('stderr') or '').strip()}</pre>
  </details>
</body>"""
    return HTMLResponse(html, status_code=200)


