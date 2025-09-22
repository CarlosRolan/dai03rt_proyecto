#db_conn.py
import os
from contextlib import closing
import psycopg2

def get_pg_conn_reset():
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.getenv("DB_PORT", "5432")),
        dbname="postgres",                
        user=os.environ["DB_USER"],         
        password=os.environ["DB_PASS"],
        sslmode=os.getenv("DB_SSLMODE", "require"),
    )
    conn.autocommit = True                 
    return conn

def reset_tmdb():
    
    fb = {"terminated": False, "dropped": False, "created": False, "time_reset": False, "error": None}
    
    try:
        with closing(get_pg_conn_reset()) as conn, conn.cursor() as cur:
            cur.execute("SET statement_timeout = '60s'; SET lock_timeout = '2s';")
            cur.execute("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = 'tmdb' AND pid <> pg_backend_pid();
            """)
            fb["terminated"] = True
            cur.execute("DROP DATABASE IF EXISTS tmdb;")
            fb["dropped"] = True
            cur.execute("CREATE DATABASE tmdb OWNER postgres;")
            fb["created"] = True
            cur.execute("RESET statement_timeout; RESET lock_timeout;")
            fb["time_reset"] = True
    except psycopg2.Error as e:
        fb["error"] = e.pgerror or str(e)
    except Exception as e:
        fb["error"] = str(e)
    return fb

def admin_conn(autocommit=False):
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.getenv("DB_PORT","5432")),
        dbname=os.environ["DB_NAME"],                  
        user=os.environ["DB_USER"],         
        password=os.environ["DB_PASS"],    
        sslmode=os.getenv("DB_SSLMODE","require"),
    )
    conn.autocommit = autocommit
    return conn

# db_conn.py (añade esta función)
import os
import psycopg2

def get_tmdb_admin_conn(autocommit: bool = False):
    """
    Connect to the 'tmdb' database as the admin user (default: 'postgres').
    Autocommit is disabled by default to keep transactional behavior.
    """
    host = os.environ["DB_HOST"]
    port = int(os.getenv("DB_PORT", "5432"))
    dbname = "tmdb"  # target DB for grants/DDL as admin
    admin_user = os.getenv("ADMIN_USER", "postgres")  # allows overriding if needed
    sslmode = os.getenv("DB_SSLMODE", "require")
    password = os.environ["DB_PASS"]  # password for admin user

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=admin_user,
        password=password,
        sslmode=sslmode,
        connect_timeout=10,
        application_name=os.getenv("APP_NAME", "tmdb_admin_conn"),
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
    )
    conn.autocommit = autocommit
    return conn


def create_role_if_needed(role_name: str = "user", role_password: str = "12341234"):
    fb = {"role": role_name, "created": False, "error": None}
    try:
        with closing(admin_conn(autocommit=True)) as conn, conn.cursor() as cur:
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role_name}') THEN
                        CREATE ROLE "{role_name}" WITH LOGIN PASSWORD '{role_password}';
                    END IF;
                END$$;
            """)
            fb["created"] = True
    except psycopg2.Error as e:
        fb["error"] = e.pgerror or str(e)
    except Exception as e:
        fb["error"] = str(e)
    return fb

def grant_tmdb_privs(role_name: str = "user", dbname: str = "tmdb"):
    fb = {"db": dbname, "role": role_name, "granted": False, "error": None}
    try:
        with closing(get_tmdb_admin_conn(autocommit=False)) as conn, conn.cursor() as cur:
            # 1) Limitar quién entra en la DB
            cur.execute(f'REVOKE CONNECT ON DATABASE {dbname} FROM PUBLIC;')
            cur.execute(f'GRANT CONNECT, TEMP ON DATABASE {dbname} TO "{role_name}";')

            # 2) Endurecer schema 'public' y dar permisos a 'user'
            cur.execute('REVOKE CREATE ON SCHEMA public FROM PUBLIC;')
            cur.execute(f'GRANT USAGE, CREATE ON SCHEMA public TO "{role_name}";')

            conn.commit()
            fb["granted"] = True
    except psycopg2.Error as e:
        fb["error"] = e.pgerror or str(e)
    except Exception as e:
        fb["error"] = str(e)
    return fb
