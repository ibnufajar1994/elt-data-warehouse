CREATE SCHEMA IF NOT EXISTS final;

CREATE TABLE final.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id text NOT NULL,
    customer_unique_id text,
    customer_zip_code_prefix integer,
    customer_city text,
    customer_state text,
    valid_from timestamp,
    valid_to timestamp,
    is_current boolean
);

CREATE TABLE final.dim_geolocation (
    geolocation_key SERIAL PRIMARY KEY,
    geolocation_zip_code_prefix integer NOT NULL,
    geolocation_lat real,
    geolocation_lng real,
    geolocation_city text,
    geolocation_state text,
    valid_from timestamp,
    valid_to timestamp,
    is_current boolean
);

CREATE TABLE final.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id text NOT NULL,
    product_category_name text,
    product_category_name_english text,
    product_name_length real,
    product_description_length real,
    product_photos_qty real,
    product_weight_g real,
    product_length_cm real,
    product_height_cm real,
    product_width_cm real,
    valid_from timestamp,
    valid_to timestamp,
    is_current boolean
);

CREATE TABLE final.dim_sellers (
    seller_key SERIAL PRIMARY KEY,
    seller_id text NOT NULL,
    seller_zip_code_prefix integer,
    seller_city text,
    seller_state text,
    valid_from timestamp,
    valid_to timestamp,
    is_current boolean
);

CREATE TABLE final.dim_dates (
    date_key SERIAL PRIMARY KEY,
    date date NOT NULL,
    year integer,
    month integer,
    day integer,
    quarter integer,
    is_weekend boolean,
    is_holiday boolean
);

CREATE TABLE final.fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id text NOT NULL,
    customer_key integer REFERENCES final.dim_customer(customer_key),
    order_status text,
    order_purchase_timestamp timestamp,
    order_approved_at timestamp,
    order_delivered_carrier_date timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp
);

CREATE TABLE final.fact_order_items (
    order_item_key SERIAL PRIMARY KEY,
    order_key integer REFERENCES final.fact_orders(order_key),
    product_key integer REFERENCES final.dim_products(product_key),
    seller_key integer REFERENCES final.dim_sellers(seller_key),
    order_item_id integer,
    shipping_limit_date timestamp,
    price real,
    freight_value real
);

CREATE TABLE final.fact_order_payments (
    payment_key SERIAL PRIMARY KEY,
    order_key integer REFERENCES final.fact_orders(order_key),
    payment_sequential integer,
    payment_type text,
    payment_installments integer,
    payment_value real
);

CREATE TABLE final.fact_order_reviews (
    review_key SERIAL PRIMARY KEY,
    order_key integer REFERENCES final.fact_orders(order_key),
    review_id text,
    review_score integer,
    review_comment_title text,
    review_comment_message text,
    review_creation_date timestamp
);

ALTER TABLE final.dim_geolocation 
    ADD CONSTRAINT unique_geolocation_zip_code_prefix UNIQUE (geolocation_zip_code_prefix);

ALTER TABLE final.dim_customer  
    ADD CONSTRAINT unique_customer_id UNIQUE (customer_id);

ALTER TABLE final.dim_products
    ADD CONSTRAINT unique_product_id UNIQUE (product_id);

ALTER TABLE final.dim_sellers 
    ADD CONSTRAINT unique_seller_id UNIQUE (seller_id);

ALTER TABLE final.dim_dates 
    ADD CONSTRAINT unique_date UNIQUE (date);

ALTER TABLE final.fact_orders 
    ADD CONSTRAINT unique_order_id UNIQUE (order_id);

ALTER TABLE final.fact_order_items 
    ADD CONSTRAINT unique_order_item UNIQUE (order_key, order_item_id);

ALTER TABLE final.fact_order_payments 
ADD CONSTRAINT unique_order_payment UNIQUE (order_key, payment_sequential);

ALTER TABLE final.fact_order_reviews 
	ADD CONSTRAINT unique_order_review UNIQUE (order_key, review_id); 