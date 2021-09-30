create table archive_raw_category(
    id VARCHAR(500) UNIQUE,
    title VARCHAR(500),
    kind VARCHAR(500),
    etag VARCHAR(500),
    channel_id VARCHAR(500),
    assignable BOOLEAN,
    country VARCHAR(500)
);
