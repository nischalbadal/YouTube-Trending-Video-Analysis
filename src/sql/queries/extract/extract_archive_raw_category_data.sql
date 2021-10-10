INSERT INTO archive_raw_category(id, title, kind, etag, channel_id, assignable, country)
SELECT id, title, kind, etag, channel_id, assignable, country FROM raw_category
ON CONFLICT(id)
DO UPDATE SET
    country = EXCLUDED.country || ';' || archive_raw_category.country;

    