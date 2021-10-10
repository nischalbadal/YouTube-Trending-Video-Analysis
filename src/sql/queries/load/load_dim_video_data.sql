INSERT INTO dim_video(client_video_id, title, channel_id, category_id, publish_date, publish_time, no_of_tags, comments_disabled,
                      ratings_disabled, video_error_or_removed, description)
select distinct
    v.video_id as client_video_id,
    v.title as title,
    c.channel_id as channel_id,
    dc.category_id as category_id,
    cast(v.publish_time as DATE) as publish_date,
    cast(v.publish_time::timestamp::time as TIME) as publish_time,
    array_length(
        regexp_split_to_array(replace(replace (tags, '"', ''),'|',','), ',')
        , 1
      ) as no_of_tags,
     cast(v.comments_disabled as BOOLEAN) as comments_disabled,
     cast(v.ratings_disabled as BOOLEAN) as ratings_disabled,
     cast(v.video_error_or_removed as BOOLEAN) as video_error_or_removed,
        description
from raw_video v
join dim_channel c on v.channel_title = c.channel_name
join dim_category dc on v.category_id = dc.client_category_id;
