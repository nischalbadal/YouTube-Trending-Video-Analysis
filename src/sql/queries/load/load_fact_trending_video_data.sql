INSERT INTO fact_trending_video(video_id, country_id, trending_date, views, likes, dislike, cmt_count)
select
    dv.id as video_id,
    dc.country_id as country_id,
    dd.date_id as trending_date,
    cast(rv.views as INT) as views,
    cast(rv.likes as INT) as likes,
    cast(rv.dislikes as INT) as dislikes,
    cast(rv.comment_count as INT) as comment_count
from raw_video rv
join dim_video dv on rv.video_id = dv.client_video_id
join dim_country dc on dc.country_code = rv.country
join dim_date dd on TO_DATE(rv.trending_date ,'YY-DD-MM') = dd.date;

