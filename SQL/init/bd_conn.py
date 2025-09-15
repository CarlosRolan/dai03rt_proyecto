# db_conn.py
import os
import psycopg2
import psycopg2.extras

from typing import Optional, Sequence, Any, List, Dict, Union

def execute_query_DDL(conn, ddl_sql: str, statement_timeout: int = 60000) -> bool:

    cur = conn.cursor()
    prev_autocommit = conn.autocommit
    try:
        # Enable autocommit just for this DDL (needed e.g. for CREATE DATABASE)
        conn.autocommit = True
        # Límites para que no se quede colgado
        # máximo de tiempo de ejecución de cada sentencia
        """cur.execute(f"SET LOCAL statement_timeout = '{25000}ms';")"""
        #Límite de espera para adquirir locks (tabla/fila/índice).
        """cur.execute(f"SET LOCAL lock_timeout = '{2000}ms';")"""
        #Sesión puede estar inactiva tras iniciar una transacción.
        #Si te quedas “en BEGIN” sin hacer nada, el servidor aborta la transacción y libera locks.
        #Evita transacciones zombi. Un valor típico: 30s.
        """cur.execute(f"SET LOCAL idle_in_transaction_session_timeout = '{30000}ms';")"""

        cur.execute(ddl_sql)
        #conn.commit()
        return True, ddl_sql

    #Para excepciones de postgres
    except psycopg2.Error as e:
        # Build a clear postgres error message
        msg = e.pgerror or (getattr(e, "diag", None) and getattr(e.diag, "message_primary", None)) or str(e)
        return False, str(msg)
    #PAra otras excepciones
    except Exception as e:
        return False, str(e)
    finally:
        # Reset modified settings and restore autocommit
        try:
            cur.execute("RESET statement_timeout;")
            cur.execute("RESET lock_timeout;")
            cur.execute("RESET idle_in_transaction_session_timeout;")
        except Exception:
            pass
        cur.close()
        conn.autocommit = prev_autocommit



def execute_query_DML(
    conn,
    sql: str,
    statement_timeout: int = 25_000,
    lock_timeout: int = 2_000,
    idle_in_transaction_session_timeout: int = 30_000
) -> Union[int, List[Dict[str, Any]], str]:
    """
    Execute DML/SELECT. If the statement returns rows (SELECT or ... RETURNING),
    return list[dict]; otherwise return affected rowcount.
    On error, rollback and return the PostgreSQL error message as str.
    """
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

def execute_sql_file(conn, file_path: str, q_type: str, timeout_ms: int = 60000):
    with open(file_path, "r", encoding="utf-8") as f:
        sql = f.read()

    if q_type == "DML":
        return execute_query_DML(conn, sql, statement_timeout=timeout_ms)
    elif q_type == "DDL":
        return execute_query_DDL(conn, sql, statement_timeout=timeout_ms)
    else:
        return "Invalid query type"


def get_psycopg2_ver():
    return {"psycopg2": psycopg2.__version__}

def get_pg_conn(autocommit: bool = False) -> psycopg2.extensions.connection:
    
    host = os.environ["DB_HOST"]            # Use your RDS Proxy endpoint here
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
