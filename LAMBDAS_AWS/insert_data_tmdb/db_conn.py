# db_conn.py
import os
from contextlib import closing
import psycopg2
import psycopg2.extras

from typing import Any, List, Dict, Union

# All comments in English
import psycopg2
import psycopg2.extras

from typing import Any, List, Dict, Union

def execute_query_DML(
    conn,
    sql: str,
    statement_timeout: int = 25_000,
    lock_timeout: int = 2_000,
    idle_in_transaction_session_timeout: int = 30_000,
    return_mode: str = "raw",  # "raw" (compat) | "friendly"
) -> Union[int, List[Dict[str, Any]], str, Dict[str, Any]]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    head = (sql or "").lstrip().split(None, 1)[0].upper() if sql else ""
    try:
        cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout}ms';")
        cur.execute(f"SET LOCAL lock_timeout = '{lock_timeout}ms';")
        cur.execute(f"SET LOCAL idle_in_transaction_session_timeout = '{idle_in_transaction_session_timeout}ms';")

        cur.execute(sql)

        if cur.description:  # hay filas (e.g., ... RETURNING)
            rows = [dict(r) for r in cur.fetchall()]
            affected = cur.rowcount
            conn.commit()
            if return_mode == "friendly":
                return {
                    "ok": True,
                    "type": head or "DML",
                    "affected": affected,
                    "rows": rows,
                    "message": f"{head or 'DML'} ejecutado con éxito ({affected} filas).",
                }
            return rows
        else:
            affected = cur.rowcount
            conn.commit()
            if return_mode == "friendly":
                if head == "INSERT":
                    msg = f"Insertadas con éxito ({affected} filas)."
                elif head == "UPDATE":
                    msg = f"Modificadas con éxito ({affected} filas)."
                elif head == "DELETE":
                    msg = f"Eliminadas con éxito ({affected} filas)."
                else:
                    msg = f"Operación {head or 'DML'} ejecutada con éxito ({affected} filas)."
                return {"ok": True, "type": head or "DML", "affected": affected, "rows": [], "message": msg}
            return affected

    except psycopg2.Error as e:
        conn.rollback()
        msg = e.pgerror or (getattr(e, "diag", None) and getattr(e.diag, "message_primary", None)) or str(e)
        if return_mode == "friendly":
            return {"ok": False, "type": head or "DML", "error": msg}
        return str(msg)

    finally:
        try:
            cur.execute("RESET statement_timeout;")
            cur.execute("RESET lock_timeout;")
            cur.execute("RESET idle_in_transaction_session_timeout;")
        except Exception:
            pass
        cur.close()

from contextlib import closing

def execute_sql_file(conn, file_path: str, timeout_ms: int = 60_000) -> str:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            sql = f.read()
    except Exception as e:
        return str(e)

    return execute_query_DML(
        conn,
        sql,
        statement_timeout=timeout_ms,
        lock_timeout=5_000,
        idle_in_transaction_session_timeout=30_000,
        return_mode="friendly"
    )

def insert_mock_data(conn):
    # El orden importa porque las tablas tienen FKs
    r1 = execute_sql_file(conn, "./SQL/insert_genres.sql")
    r2 = execute_sql_file(conn, "./SQL/insert_movies.sql")
    r3 = execute_sql_file(conn, "./SQL/insert_movie_genres.sql")

    return {
        "genres": r1,
        "movies": r2,
        "movie_genres": r3
    }
        
def get_psycopg2_ver():
    return {"psycopg2": psycopg2.__version__}

def get_pg_conn(autocommit: bool = False) -> psycopg2.extensions.connection:
    
    host = os.environ["DB_HOST"]            
    port = int(os.getenv("DB_PORT", "5432"))
    dbname = os.environ["DB_NAME"]
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASS"]

    sslmode = os.getenv("DB_SSLMODE", "require")   # 'require' | 'verify-ca' | 'verify-full'
    # Esto es para la clave pero no es necesario si uso require
    #sslrootcert = os.getenv("DB_SSLROOTCERT")      # path to CA bundle if using verify-*

    # Base kwargs
    kwargs = dict(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=10,              # Fail fast if networking is wrong
        application_name=os.getenv("APP_NAME", "lambda_init_db"),
        # TCP keepalives help across NAT and idle Lambda containers
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
        sslmode=sslmode,
    )
    # Only when using verify-ca/verify-full kwargs
    # if sslrootcert: ["sslrootcert"] = sslrootcert

    conn = psycopg2.connect(**kwargs)
    conn.autocommit = False        
    return conn

# Esto lo quiero dejar asi por si acaso en el futuro necesiot una conecxion como admin, ya se que es un fallo de seguirdad
def admin_conn(autocommit=False):
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=5432,
        dbname="postgres",
        user="postgres",      
        password="12341234",
        sslmode=os.getenv("DB_SSLMODE","require"),
    )
    conn.autocommit = autocommit
    return conn