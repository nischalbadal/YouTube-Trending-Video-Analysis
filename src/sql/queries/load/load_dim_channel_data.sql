INSERT INTO dim_channel(channel_name)
SELECT DISTINCT channel_title FROM raw_video;
