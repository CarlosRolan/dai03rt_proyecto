-- ./SQL/clean.sql
-- Limpieza segura del esquema TMDB en 'public'
-- Orden: tablas puente (N:M) -> tablas base

-- Si en el futuro creas vistas/materializadas u otras dependencias
-- sobre estas tablas, elimínalas antes o añade aquí sus DROP.

DROP TABLE IF EXISTS public.movie_genres;
DROP TABLE IF EXISTS public.movie_actors;

DROP TABLE IF EXISTS public.genres;
DROP TABLE IF EXISTS public.actors;
DROP TABLE IF EXISTS public.movies;

-- Notas:
-- - No incluimos BEGIN/COMMIT: tu execute_schema_ddl ya envuelve todo en una transacción.
-- - Los índices asociados se eliminan automáticamente al hacer DROP TABLE.
-- - No usamos CASCADE para evitar borrar objetos imprevistos; el orden de borrado resuelve dependencias.

