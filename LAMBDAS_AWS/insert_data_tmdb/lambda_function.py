import json
from contextlib import closing
from db_conn import get_pg_conn, execute_query_DML, insert_mock_data, execute_sql_file

def lambda_handler(event, context):
    # Si no viene QUERY o viene vacía => insertamos mock data
    sql = (event.get("QUERY") or "").strip()

    if sql == "insert_movie_actors":
        res = ed = cur.rowcount
        conn.commit()

    # Limpia temporal
    try:
        os.remove(tmp_path)
    except Exception:
        pass

    return {
…        cur.execute("SET LOCAL idle_in_transaction_session_timeout = '120000ms';")

        cur.execute(create_staging_sql)
        cur.copy_expert(copy_sql, fh)
        cur.execute(insert_sql)
        inserted = cur.rowcount
        conn.commit()

    return {"ok": True, "inserted": inserted}copy_local_movie_actors_to_db()
        return {"statusCode": 200 if res["ok"] else 500, "body": res}

    if sql == "insert_movies":
        res = copy_csv_file_to_movies_with_upsert_flexible(
            file_path= "movies.csv",
        )
        return {"statusCode": 200 if res["ok"] else 500, "body": res}

    file_executed = False
    with closing(get_pg_conn()) as conn:
        if not sql:
            file_executed = True
            # Inserta mocks (usa execute_sql_file en modo "friendly" por debajo)
            #result = insert_mock_data(conn)
            status = 200
        else:
            # Ejecuta SIEMPRE DML (INSERT/UPDATE) en modo "friendly"
            result = execute_query_DML(
                conn,
                sql,
                statement_timeout=120_000,          # ajusta si quieres
                lock_timeout=5_000,
                idle_in_transaction_session_timeout=30_000,
                return_mode="friendly"
            )
            # result: {"ok": True/False, "type": "INSERT"/"UPDATE", "affected": N, ...}
            status = 200 if isinstance(result, dict) and result.get("ok", False) else 500

    return {
        "statusCode": status,
        "body": {
            "file_executed": file_executed,  # True si se ejecutaron los .sql de mock
            "result": result                  # dict "friendly" o dict con mocks
        }
    }
import os
import csv
import gzip
import re
from contextlib import closing
from typing import List

from db_conn import get_pg_conn

# Columnas esped = cur.rowcount
        conn.commit()

    # Limpia temporal
    try:
        os.remove(tmp_path)
    except Exception:
        pass

    return {
…        cur.execute("SET LOCAL idle_in_transaction_session_timeout = '120000ms';")

        cur.execute(create_staging_sql)
        cur.copy_expert(copy_sql, fh)
        cur.execute(insert_sql)
        inserted = cur.rowcount
        conn.commit()

    return {"ok": True, "inserted": inserted}eradas en la tabla destino
EXPECTED = [
    "id", "title", "popularity", "vote_average", "runtime",
    "budget", "revenue", "overview", "release_date", "success"
]

def _sanitize_csv_to_temp(input_path: str, temp_out: str) -> dict:
    """
    Reescribe el CSV a 'temp_out' garantizando que cada fila tenga
    el mismo nº de columnas que el header del archivo.
    - Rellena con '' las columnas que falten al final de la fila.
    - Recorta si sobran columnas.
    - Limpia caracteres problemáticos (terminadores inusuales, null byte, controles).
    Devuelve: {"cols": [..], "n_rows": N}
    """
    is_gz = input_path.lower().endswith(".gz")
    open_text = (lambda p: gzip.open(p, mode="rt", encoding="utf-8", newline="")) if is_gz \
                else (lambda p: open(p, mode="rt", encoding="utf-8", newline=""))

    def _clean_cell(s: str) -> str:
        if s is None:
            return ""
        s = s.replace("\r\n", "\n").replace("\r", "\n")
        s = s.replace("\u2028", " ").replace("\u2029", " ")
        s = s.replace("\x00", " ")
        s = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", " ", s)  # deja \n y \t
        return s.strip()

    with open_text(input_path) as fin, open(temp_out, "w", encoding="utf-8", newline="") as fout:
        rdr = csv.reader(fin)
        wtr = csv.writer(
            fout,
            quoting=csv.QUOTE_ALL,
            escapechar='"',
            doublequote=True,
            lineterminator="\n",
        )

        header = next(rdr, None)
        if not header:
            raise ValueError("CSV sin cabecera")
        header = [h.strip() for h in header]
        wtr.writerow(header)

        target_len = len(header)
        n = 0
        for row in rdr:
            row = [_clean_cell(c) for c in row]
            if len(row) < target_len:
                row = row + [""] * (target_len - len(row))
            elif len(row) > target_len:
                row = row[:target_len]
            wtr.writerow(row)
            n += 1

    return {"cols": header, "n_rows": n}


def copy_csv_file_to_movies_with_upsert_flexible(
    file_path: str,
    *,
    table: str = "public.movies",
    null_as: str = "",
    statement_timeout_ms: int = 600_000,
    lock_timeout_ms: int = 5_000,
    idle_in_tx_timeout_ms: int = 120_000,
) -> dict:
    """
    Carga robusta desde CSV (o CSV.gz) a public.movies:
      1) Sanea CSV a /tmp para asegurar el nº de columnas por fila = header.
      2) COPY a staging con TODAS las columnas TEXT (no casca por tipos).
         Usa NULL '' y FORCE_NULL(...) para convertir también "" a NULL.
      3) INSERT a destino casteando desde TEXT y haciendo ON CONFLICT (id) DO NOTHING.
    """
    # 1) Saneamos a /tmp
    tmp_path = "/tmp/_clean_movies.csv"
    info = _sanitize_csv_to_temp(file_path, tmp_path)
    csv_cols = info["cols"]           # columnas reales del CSV
    present = [c for c in csv_cols if c in EXPECTED]
    if "id" not in present:
        raise ValueError("La columna 'id' debe existir en el CSV")
    cols_csv_list = ", ".join(present)

    # Columnas a las que aplicamos FORCE_NULL (vacíos comillados -> NULL)
    _force_null_candidates = {
        "popularity", "vote_average", "runtime", "budget",
        "revenue", "release_date", "success"
    }
    force_null_list = [c for c in present if c in _force_null_candidates]
    force_null_clause = f", FORCE_NULL ({', '.join(force_null_list)})" if force_null_list else ""

    # 2) Staging TODO TEXT para evitar errores de tipos en COPY
    #    (aunque falten columnas en el CSV, existen como TEXT y quedarán NULL tras el COPY)
    create_staging_sql = """
        CREATE TEMP TABLE _stg_movies (
            id            TEXT,
            title         TEXT,
            popularity    TEXT,
            vote_average  TEXT,
            runtime       TEXT,
            budget        TEXT,
            revenue       TEXT,
            overview      TEXT,
            release_date  TEXT,
            success       TEXT
        ) ON COMMIT DROP;
    """

    copy_sql = (
        f"COPY _stg_movies ({cols_csv_list}) "
        f"FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '\"', ESCAPE '\"', NULL ''{force_null_clause});"
    )

    # 3) Completar/castear desde TEXT y UPSERT
    #    Ojo: como staging es TEXT, aquí sí tiene sentido NULLIF(col,'')::tipo
    select_sql = f"""
        SELECT
            NULLIF(id,'')::bigint                               AS id,
            COALESCE(title,'')                                  AS title,
            NULLIF(popularity,'')::double precision             AS popularity,
            NULLIF(vote_average,'')::numeric                    AS vote_average,
            NULLIF(runtime,'')::integer                         AS runtime,
            NULLIF(budget,'')::bigint                           AS budget,
            NULLIF(revenue,'')::bigint                          AS revenue,
            COALESCE(overview,'')                               AS overview,
            NULLIF(release_date,'')::date                       AS release_date,
            COALESCE(NULLIF(success,''),'false')::boolean       AS success
        FROM _stg_movies
        WHERE NULLIF(id,'') IS NOT NULL
    """

    insert_sqled = cur.rowcount
        conn.commit()

    # Limpia temporal
    try:
        os.remove(tmp_path)
    except Exception:
        pass

    return {
…        cur.execute("SET LOCAL idle_in_transaction_session_timeout = '120000ms';")

        cur.execute(create_staging_sql)
        cur.copy_expert(copy_sql, fh)
        cur.execute(insert_sql)
        inserted = cur.rowcount
        conn.commit()

    return {"ok": True, "inserted": inserted} = (
        f"INSERT INTO {table} ({', '.join(EXPECTED)})\n"
        f"{select_sql}\n"
        f"ON CONFLICT (id) DO NOTHING;"
    )

    # 4) Ejecutar
    with closing(get_pg_conn()) as conn, conn.cursor() as cur, open(tmp_path, "r", encoding="utf-8", newline="") as fh:
        cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout_ms}ms';")
        cur.execute(f"SET LOCAL lock_timeout = '{lock_timeout_ms}ms';")
        cur.execute(f"SET LOCAL idle_in_transaction_session_timeout = '{idle_in_tx_timeout_ms}ms';")

        cur.execute(create_staging_sql)
        cur.copy_expert(copy_sql, fh)
        cur.execute(insert_sql)
        inserted = cur.rowcount
        conn.commit()

    # Limpia temporal
    try:
        os.remove(tmp_path)
    except Exception:
        pass

    return {
        "ok": True,
        "inserted": inserted,
        "csv_cols": present,
        "rows_in_csv": info["n_rows"],
        "force_null_applied_on": force_null_list,
    }

# --- mínimo necesario ---

from contextlib import closing
from db_conn import get_pg_conn  # ya lo tienes en tu lambda

CSV_MOVIE_ACTORS_PATH = "/var/task/movie_actors.csv"  # <--- RUTA HARDCODEADA

def copy_local_movie_actors_to_db() -> dict:
    """
    Lee /var/task/movie_actors.csv (dos columnas con header: movie_id,actor_id)
    y hace COPY -> staging TEXT -> INSERT casteando a BIGINT con ON CONFLICT DO NOTHING.
    """
    create_staging_sql = """
        CREATE TEMP TABLE _stg_movie_actors (
            movie_id  TEXT,
            actor_id  TEXT
        ) ON COMMIT DROP;
    """

    copy_sql = (
        "COPY _stg_movie_actors (movie_id, actor_id) "
        "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '\"', ESCAPE '\"', NULL '', FORCE_NULL (movie_id, actor_id));"
    )

    select_sql = """
        SELECT
            NULLIF(movie_id,'')::bigint AS movie_id,
            NULLIF(actor_id,'')::bigint AS actor_id
        FROM _stg_movie_actors
        WHERE NULLIF(movie_id,'') IS NOT NULL
          AND NULLIF(actor_id,'') IS NOT NULL
    """

    insert_sql = (
        "INSERT INTO public.movie_actors (movie_id, actor_id)\n"
        f"{select_sql}\n"
        "ON CONFLICT (movie_id, actor_id) DO NOTHING;"
    )

    with closing(get_pg_conn()) as conn, conn.cursor() as cur, open("movie_actors.csv", "r", encoding="utf-8", newline="") as fh:
        # timeouts razonables (ajusta si quieres)
        cur.execute("SET LOCAL statement_timeout = '600000ms';")
        cur.execute("SET LOCAL lock_timeout = '5000ms';")
        cur.execute("SET LOCAL idle_in_transaction_session_timeout = '120000ms';")

        cur.execute(create_staging_sql)
        cur.copy_expert(copy_sql, fh)
        cur.execute(insert_sql)
        inserted = cur.rowcount
        conn.commit()

    return {"ok": True, "inserted": inserted}