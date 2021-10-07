CREATE TABLE dim_category(
    id SERIAL PRIMARY KEY,
    category_id VARCHAR(200),
    category_title VARCHAR(200),
    assignable BOOLEAN
);
