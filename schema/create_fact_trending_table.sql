CREATE TABLE fact_trending_video(
trending_video_id SERIAL PRIMARY KEY,
video_id INT,
country_id INT,
trending_date INT,
views INT,
likes INT,
dislike INT,
cmt_count INT,
CONSTRAINT fk_video_id FOREIGN KEY (video_id)
REFERENCES dim_video(id) ON DELETE CASCADE,
CONSTRAINT fk_country_id FOREIGN KEY (country_id)
REFERENCES dim_country(country_id) ON DELETE CASCADE,
CONSTRAINT fk_trending_date FOREIGN KEY (trending_date)
REFERENCES dim_date(date_id) ON DELETE CASCADE
);

