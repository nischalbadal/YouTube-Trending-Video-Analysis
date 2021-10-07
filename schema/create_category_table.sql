CREATE TABLE category (
    id SERIAL PRIMARY KEY,
    client_category_id VARCHAR(200),
    category_title VARCHAR(200),
    assignable BOOLEAN
);
