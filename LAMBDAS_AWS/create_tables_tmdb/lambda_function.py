import json
from db_conn import execute_schema_file, execute_schema_ddl

def lambda_handler(event, context):
    
    clean_first = event.get("clean_first", True)

    result = execute_schema_file(file_path="./SQL/tables.sql", clean_first=clean_first)

    return result

