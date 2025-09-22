# db_conn.py
import os
from contextlib import closing
import psycopg2
import psycopg2.extras

from typing import Any, List, Dict, Union

def execute_query_DML(
    conn,
    sql: str,
    statement_timeout: int = 25_000,
    lock_timeout: int = 2_000,
    idle_in_transaction_session_timeout: int = 30_000
) -> Union[int, List[Dict[str, Any]], str]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        # Límites para que no se quede colgado
        # máximo de tiempo de ejecución de cada sentencia
        cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout}ms';")
        #Límite de espera para adquirir locks (tabla/fila/índice).
        cur.execute(f"SET LOCAL lock_timeout = '{lock_timeout}ms';")
        #Sesión puede estar inactiva tras iniciar una transacción.
        #Si te quedas “en BEGIN” sin hacer nada, el servidor aborta la transacción y libera locks.
        #Evita transacciones zombi. Un valor típico: 30s.
        cur.execute(f"SET LOCAL idle_in_transaction_session_timeout = '{idle_in_transaction_session_timeout}ms';")

        cur.execute(sql)

        if cur.description:  # rows available (SELECT / VIEW / ... RETURNING)
            rows = cur.fetchall()
            conn.commit()
            return [dict(r) for r in rows]
        else:
            affected = cur.rowcount
            conn.commit()
            return affected

    except psycopg2.Error as e:
        # Ensure connection isn't left in aborted state
        conn.rollback()
        # Prefer server-provided message; fallback to str(e)
        msg = e.pgerror or (getattr(e, "diag", None) and getattr(e.diag, "message_primary", None)) or str(e)
        return str(msg)

    finally:
        try:
            cur.execute("RESET statement_timeout;")
            cur.execute("RESET lock_timeout;")
            cur.execute("RESET idle_in_transaction_session_timeout;")
        except Exception:
            pass
        cur.close()
             
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
        password=os.environ["DB_PASS"],
        sslmode=os.getenv("DB_SSLMODE","require"),
    )
    conn.autocommit = autocommit
    return conn