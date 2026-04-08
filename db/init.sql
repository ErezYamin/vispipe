DO $$ BEGIN
    CREATE TYPE image_status AS ENUM ('pending', 'processed', 'failed');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS images (
    image_id        UUID PRIMARY KEY,
    source_path     TEXT NOT NULL,
    processed_path  TEXT,
    status          image_status NOT NULL DEFAULT 'pending',
    checksum        TEXT,
    width           INT,
    height          INT,
    channels        INT,
    file_size_bytes INT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ
);
