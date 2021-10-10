CREATE TABLE dim_country(
    country_id SERIAL PRIMARY KEY,
    country_code VARCHAR(100) UNIQUE,
    country_name VARCHAR(200)
);
