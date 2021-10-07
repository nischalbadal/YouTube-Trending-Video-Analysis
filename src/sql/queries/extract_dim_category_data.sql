INSERT INTO dim_category (category_id, category_title, assignable)
SELECT client_category_id, category, assignable FROM category;
