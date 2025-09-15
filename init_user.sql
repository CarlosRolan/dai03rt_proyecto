-- Creación del rol de uso común para la interaccion con la base de datos
DO $ $ BEGIN IF NOT EXISTS (
    SELECT
        1
    FROM
        pg_roles
    WHERE
        rolname = 'user'
) THEN CREATE ROLE "user" WITH LOGIN PASSWORD '1234134';

END IF;

END $ $;