# lambda_function.py
import json
from contextlib import closing
from decimal import Decimal
from datetime import date, datetime
from db_conn import get_pg_conn, execute_query_DML

def to_jsonable(obj):
    # Recursive conversion for lists/dicts
    if isinstance(obj, dict):
        return {k: to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_jsonable(x) for x in obj]
    if isinstance(obj, Decimal):
        return float(obj)  # or str(obj) if you need exactness
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    return obj

def _toret(query, result, ok):
    return {
        "query": query,
        "result": to_jsonable(result),
        "ok": ok
    }

def lambda_handler(event, context):
    query = event.get("QUERY") or "SELECT 1 AS ok;"

    if query == "SQL_IMPOSIBLE":
        return _toret(query, "SQL_IMPOSIBLE", "NOT_INFO")

    try:
        with closing(get_pg_conn()) as conn:
            result = execute_query_DML(conn, query)
        # Return plain Python, already JSON-safe
        return _toret(query, result, True)
    except Exception as e:
        return _toret(query, str(e), False)