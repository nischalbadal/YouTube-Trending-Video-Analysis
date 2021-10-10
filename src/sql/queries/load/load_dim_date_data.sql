INSERT INTO dim_date(date, day_of_week)
select distinct
TO_DATE(trending_date ,'YY-DD-MM') as date,
to_char(TO_DATE(trending_date ,'YY-DD-MM'), 'Day') as day_of_week
from raw_video order by TO_DATE(trending_date ,'YY-DD-MM') asc ;

