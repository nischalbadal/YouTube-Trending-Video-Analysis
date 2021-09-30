create table raw_video(
    video_id VARCHAR(500) UNIQUE,
    trending_date VARCHAR(500),
    title VARCHAR(500),
    channel_title VARCHAR(500),
    category_id VARCHAR(500),
    publish_time VARCHAR(500),
    tags VARCHAR(500),
    views VARCHAR(500),
    likes VARCHAR(500),
    dislikes VARCHAR(500),
    comment_count VARCHAR(500),
    thumbnail_link VARCHAR(500),
    comments_disabled VARCHAR(500),
    ratings_disabled VARCHAR(500),
    video_error_or_removed VARCHAR(500),
    description TEXT,
    country VARCHAR(500)
);

