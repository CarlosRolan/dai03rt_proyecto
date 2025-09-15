SELECT
    pg_terminate_backend(pid)
FROM
    pg_stat_activity
WHERE
    datname = 'tmdb'
    AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS tmdb;

CREATE DATABASE tmdb OWNER postgres;