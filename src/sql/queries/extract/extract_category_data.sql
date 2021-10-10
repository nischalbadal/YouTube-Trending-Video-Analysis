INSERT INTO category(client_category_id, category_title, assignable)
SELECT
id as client_category_id,
title as category_title,
CAST(assignable as BOOLEAN)
FROM raw_category;
