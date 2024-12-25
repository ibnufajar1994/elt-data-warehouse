CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE SCHEMA IF NOT EXISTS final;

CREATE TABLE final.dim_customers (
    customer_id_sk UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    customer_id_nk text NOT NULL,
    customer_unique_id text,
    customer_zip_code_prefix integer,
    customer_city text,
    customer_state text,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    current_flag VARCHAR(10) DEFAULT 'current'
);

CREATE TABLE final.dim_geolocation (
    geolocation_sk UUID DEFAULT uuid_generate_v4()  PRIMARY KEY,
    geolocation_zip_code_prefix integer NOT NULL,
    geolocation_lat float,
    geolocation_lng float,
    geolocation_city text,
    geolocation_state text,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    current_flag VARCHAR(10) DEFAULT 'current'
);


CREATE TABLE final.dim_products (
    product_id_sk UUID DEFAULT uuid_generate_v4()  PRIMARY KEY,
    product_id_nk text NOT NULL,
    product_category_name text,
    product_category_name_english text,
    product_name_length real,
    product_description_length real,
    product_photos_qty real,
    product_weight_g real,
    product_length_cm real,
    product_height_cm real,
    product_width_cm real,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    current_flag VARCHAR(10) DEFAULT 'current'
);

CREATE TABLE final.dim_sellers (
    seller_id_sk UUID DEFAULT uuid_generate_v4()  PRIMARY KEY,
    seller_id_nk text NOT NULL,
    seller_zip_code_prefix integer,
    seller_city text,
    seller_state text,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    current_flag VARCHAR(10) DEFAULT 'current'
);


CREATE TABLE final.dim_reviews(
    review_id_sk UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    review_id_nk text NOT NULL,
    order_id_nk text NOT NULL,
    review_comment_title text,
    review_comment_message text,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

DROP TABLE if exists final.dim_date;
CREATE TABLE final.dim_date
(
  date_id              INT NOT null primary KEY,
  date_actual              DATE NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             VARCHAR(20) NOT NULL
);

CREATE INDEX dim_date_date_actual_idx
  ON final.dim_date(date_actual);

CREATE TABLE final.fact_order_line_items (
    order_id_sk UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    order_id_nk text NOT NULL,
    product_id_sk uuid REFERENCES final.dim_products(product_id_sk),
    customer_id_sk uuid REFERENCES final.dim_customers(customer_id_sk),
    seller_id_sk uuid REFERENCES final.dim_sellers(seller_id_sk),
    date_id INT REFERENCES final.dim_date(date_id),
    price float NOT NULL,
    freight_value float NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

    
);

CREATE TABLE final.fact_order_payments (
    order_id_sk UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    order_id_nk text NOT NULL,
    customer_id_sk uuid REFERENCES final.dim_customers(customer_id_sk),
    payment_sequential INT,
    payment_type text,
    payment_installments float,
    payment_value float,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);


CREATE TABLE final.fact_order_reviews (
    review_id_sk UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    review_id_nk text NOT NULL,
    order_id_nk text NOT NULL,
    customer_id_sk uuid REFERENCES final.dim_customers(customer_id_sk),
    seller_id_sk uuid REFERENCES final.dim_sellers(seller_id_sk),
    review_score INT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

INSERT INTO final.dim_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
       datum AS date_actual,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW') AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN 'weekend'
           ELSE 'weekday'
           END AS weekend_indr
FROM (SELECT '1998-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;


ALTER TABLE final.dim_reviews
ADD CONSTRAINT dim_reviews_unique UNIQUE(review_id_nk);

ALTER TABLE final.fact_order_line_items
ADD CONSTRAINT fact_order_line_items_unique UNIQUE(order_id_nk);

ALTER TABLE final.fact_order_payments
ADD CONSTRAINT fact_order_payments_unique UNIQUE(order_id_nk);

ALTER TABLE final.fact_order_reviews
ADD CONSTRAINT fact_order_reviews_unique UNIQUE(review_id_nk);

