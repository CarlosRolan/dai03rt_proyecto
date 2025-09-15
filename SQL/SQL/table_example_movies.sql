-- TMDB movies (wide staging table)
CREATE TABLE IF NOT EXISTS tmdb_movies (
    tmdb_id INTEGER PRIMARY KEY,
    -- TMDB 'id'
    adult BOOLEAN DEFAULT FALSE,
    backdrop_path TEXT,
    belongs_to_collection JSONB,
    -- object {id,name,poster_path,backdrop_path}
    budget BIGINT,
    genres JSONB,
    -- array [{id,name}]
    genre_ids INTEGER [],
    -- from discover (ints)
    homepage TEXT,
    imdb_id TEXT,
    original_language TEXT,
    original_title TEXT,
    overview TEXT,
    popularity DOUBLE PRECISION,
    poster_path TEXT,
    production_companies JSONB,
    -- array [{id,name,origin_country,logo_path}]
    production_countries JSONB,
    -- array [{iso_3166_1,name}]
    release_date DATE,
    revenue BIGINT,
    runtime INTEGER,
    spoken_languages JSONB,
    -- array [{iso_639_1,english_name,name}]
    status TEXT,
    -- e.g. 'Released'
    tagline TEXT,
    title TEXT,
    video BOOLEAN,
    vote_average DOUBLE PRECISION,
    vote_count INTEGER,
    -- Keep the full TMDB payload for future transforms
    raw JSONB NOT NULL,
    -- Ops fields
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Helpful indexes for typical filters
CREATE INDEX IF NOT EXISTS ix_tmdb_movies_release_date ON tmdb_movies (release_date);

CREATE INDEX IF NOT EXISTS ix_tmdb_movies_popularity ON tmdb_movies (popularity DESC);

-- Text search (optional)
-- CREATE INDEX IF NOT EXISTS ix_tmdb_movies_title_tsv ON tmdb_movies
--   USING GIN (to_tsvector('simple', coalesce(title,'') || ' ' || coalesce(original_title,'')));
-- (Optional) trigger to keep updated_at fresh
-- CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
-- BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;
-- CREATE TRIGGER trg_tmdb_movies_updated BEFORE UPDATE ON tmdb_movies
-- FOR EACH ROW EXECUTE FUNCTION set_updated_at();