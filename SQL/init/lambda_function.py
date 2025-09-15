import json
from contextlib import closing
import os

from db_conn import get_pg_conn, execute_query_DML, execute_query_DDL, get_psycopg2_ver, execute_sql_file


def lambda_handler(event, context):

    RESET_DB_FILE_PATH = "./SQL/reset_db.sql" # Crea el usuario y la base de datos si no existe

    with closing(get_pg_conn()) as conn:
        
        result_body = {}

        try:
            QUERY_TYPE = event["query_type"]
            QUERY = event["query"]

            if QUERY_TYPE == "DDL":
                if QUERY.startswith("SELECT"):
                    raise ValueError(f"La query es de tipo DML pero QUERY_TYPE={QUERY_TYPE}")
                else:
                    result = execute_query_DDL(conn, sql = QUERY)
            elif QUERY_TYPE == "DML":
                if QUERY.startswith("CREATE"):
                    raise ValueError(f"La query es de tipo DDL pero QUERY_TYPE={QUERY_TYPE}")
                else:
                    result = execute_query_DML(conn, sql = QUERY)
                
        except:
            result = execute_sql_file(conn, RESET_DB_FILE_PATH, "DDL")
            result_body = {
                'query_type': "DDL",
                'query': "file..",
                'result': result
            }
            return {
                'statusCode': 200,
                'body': result_body
                }

        """if QUERY_TYPE == "DDL":
            result = execute_query_DDL(conn, sql = QUERY)
        elif QUERY_TYPE == "DML":
            result = execute_query_DML(conn, sql = QUERY)
        else:
            raise ValueError(f"QUERY_TYPE={QUERY_TYPE} no es un tipo válido (DDL o DML)")"""

    return {
        'statusCode': 200,
        'body': event
    }
