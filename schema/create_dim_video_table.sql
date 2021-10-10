CREATE TABLE dim_video(
    id SERIAL PRIMARY KEY,
	client_video_id	VARCHAR(255),
	title	TEXT,
	channel_id INT REFERENCES dim_channel(channel_id),
	category_id INT REFERENCES dim_category(category_id),
	publish_date DATE,
	publish_time TIME,
	no_of_tags BIGINT,
    comments_disabled BOOLEAN,
    ratings_disabled BOOLEAN,
    video_error_or_removed BOOLEAN,
    description TEXT
	);
	