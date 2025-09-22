-- SCHEMA: tmdb (tablas en 'public')

-- TABLE: movies
CREATE TABLE IF NOT EXISTS public.movies (
    id              INTEGER PRIMARY KEY,                      -- TMDB movie id (manual)
    title           TEXT NOT NULL,
    popularity      REAL,                                     -- float4
    vote_average    NUMERIC(3,1) 
    CHECK (vote_average >= 0 AND vote_average <= 10),     -- 0.0..10.0
    runtime         SMALLINT CHECK (runtime >= 0),            -- minutos
    budget          BIGINT CHECK (budget  >= 0),              -- entero (unidades monetarias)
    revenue         BIGINT CHECK (revenue >= 0),
    overview        TEXT,
    release_date    DATE,
    success         BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_movies_release_date ON public.movies (release_date);
CREATE INDEX IF NOT EXISTS idx_movies_popularity  ON public.movies (popularity);
CREATE INDEX IF NOT EXISTS idx_movies_title_lower ON public.movies (lower(title));

-- TABLE: actors (cast)
CREATE TABLE IF NOT EXISTS public.actors (
    id          INTEGER PRIMARY KEY,                          
    name        TEXT NOT NULL,
    age         SMALLINT CHECK (age IS NULL OR (age BETWEEN 0 AND 150)),
    gender      TEXT CHECK (gender IN ('Not set', 'Female', 'Male', 'Non-binary')),
    popularity  REAL
);

CREATE INDEX IF NOT EXISTS idx_actors_name_lower ON public.actors (lower(name));
CREATE INDEX IF NOT EXISTS idx_actors_popularity ON public.actors (popularity);


-- TABLE: genres
CREATE TABLE IF NOT EXISTS public.genres (
    id      INTEGER PRIMARY KEY,          -- TMDB genre id (manual)
    name    TEXT NOT NULL UNIQUE
);

-- TABLE: movie_actors  (credits → cast, relación N:M)
CREATE TABLE IF NOT EXISTS public.movie_actors (
    movie_id INTEGER NOT NULL REFERENCES public.movies(id) ON DELETE CASCADE,
    actor_id INTEGER NOT NULL REFERENCES public.actors(id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, actor_id)
);

-- TABLE: movie_genres  (genres, relación N:M)
CREATE TABLE IF NOT EXISTS public.movie_genres (
    movie_id INTEGER NOT NULL REFERENCES public.movies(id) ON DELETE CASCADE,
    genre_id INTEGER NOT NULL REFERENCES public.genres(id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, genre_id)
);

CREATE INDEX IF NOT EXISTS idx_movie_actors_actor ON public.movie_actors (actor_id);
CREATE INDEX IF NOT EXISTS idx_movie_genres_genre ON public.movie_genres (genre_id);
