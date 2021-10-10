INSERT INTO raw_category(id, title, kind, etag, channel_id, assignable, country)
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT(id)
DO UPDATE SET 
    country = EXCLUDED.country || ';' || raw_category.country;
