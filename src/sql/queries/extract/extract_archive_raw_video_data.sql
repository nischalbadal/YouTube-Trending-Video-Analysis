INSERT INTO archive_raw_video 
    (video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,
    dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description,country)
SELECT 
    video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,
    comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description,country
FROM raw_video;

