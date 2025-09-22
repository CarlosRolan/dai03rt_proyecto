# db_conn.py

import os
import psycopg2
from contextlib import closing
from typing import Dict, Any

def get_pg_conn():
    host = os.environ["DB_HOST"]
    port = int(os.getenv("DB_PORT", "5432"))
    dbname = os.getenv("DB_NAME", "tmdb")
    user = os.getenv("DB_USER", "user")
    sslmode = os.getenv("DB_SSLMODE", "require")
    password = os.environ["DB_PASS"]

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
        connect_timeout=10,
        application_name=os.getenv("APP_NAME", "lambda_create_tables"),
        # TCP keepalives (useful in Lambda/VPC/Proxy)
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
    )
    # Keep transactional behavior for DDL unless you explicitly opt-in to autocommit
    conn.autocommit = False
    return conn


def execute_schema_ddl(
    ddl_sql: str,
    statement_timeout_ms: int = 60000,
    lock_timeout_ms: int = 2000,
    idle_tx_timeout_ms: int = 30000
):
    """
    Open a connection, execute the multi-statement DDL in a single transaction, and close.
    Returns (True, 'ok') on success or (False, 'error message') on failure.
    """
    try:
        with closing(get_pg_conn()) as conn:
            # with conn => commit on success, rollback on exception
            with conn, conn.cursor() as cur:
                # Local timeouts apply only to this transaction
                cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout_ms}ms';")
                cur.execute(f"SET LOCAL lock_timeout = '{lock_timeout_ms}ms';")
                cur.execute(f"SET LOCAL idle_in_transaction_session_timeout = '{idle_tx_timeout_ms}ms';")
                # Optional: fix search_path if you ever drop schema qualification
                # cur.execute("SET LOCAL search_path = public;")

                # Execute the whole SQL script (multiple statements separated by ';' are fine)
                cur.execute(ddl_sql)
        return True, "ok"
    except psycopg2.Error as e:
        msg = e.pgerror or (getattr(e, "diag", None) and getattr(e.diag, "message_primary", None)) or str(e)
        return False, msg
    except Exception as e:
        return False, str(e)


def execute_schema_file(file_path: str,  clean_first: bool, timeout_ms: int = 60000) -> Dict[str, Any]:
    """
    Read SQL from file, execute DDL, then query information_schema to return public column structure.
    """
    if clean_first:
        with open("./SQL/clean.sql", "r", encoding="utf-8") as f:
            sql_clean = f.read()
        ok, msg = execute_schema_ddl(sql_clean, statement_timeout_ms=timeout_ms)
        if not ok:
            return {"success": False, "error": msg}
     
    # Read file
    with open(file_path, "r", encoding="utf-8") as f:
        sql = f.read()

    # Execute DDL
    ok, msg = execute_schema_ddl(sql, statement_timeout_ms=timeout_ms)
    if not ok:
        return {"success": False, "error": msg}

    # Fetch structure from a fresh connection
    with closing(get_pg_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name, column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position;
            """)
            cols = cur.fetchall()

    # Build structure dict
    structure = {}
    for table, col, dtype, nullable in cols:
        structure.setdefault(table, []).append({
            "column": col,
            "type": dtype,
            "nullable": nullable,
        })

    return {"success": True, "structure": structure, "cleaned": clean_first}