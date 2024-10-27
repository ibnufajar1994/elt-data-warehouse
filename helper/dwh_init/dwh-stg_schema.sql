-- Ensure the uuid-ossp extension is available for generating UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Staging Schema
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE stg.customers (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    customer_id text NOT NULL,
    customer_unique_id text,
    customer_zip_code_prefix integer,
    customer_city text,
    customer_state text,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg.geolocation (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    geolocation_zip_code_prefix integer NOT NULL,
    geolocation_lat real,
    geolocation_lng real,
    geolocation_city text,
    geolocation_state text,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg.order_items (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    order_id text NOT NULL,
    order_item_id integer NOT NULL,
    product_id text,
    seller_id text,
    shipping_limit_date text,
    price real,
    freight_value real,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg.order_payments (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    order_id text NOT NULL,
    payment_sequential integer NOT NULL,
    payment_type text,
    payment_installments integer,
    payment_value real,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg.order_reviews (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    review_id text NOT NULL,
    order_id text NOT NULL,
    review_score integer,
    review_comment_title text,
    review_comment_message text,
    review_creation_date text,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg.orders (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    order_id text NOT NULL,
    customer_id text,
    order_status text,
    order_purchase_timestamp text,
    order_approved_at text,
    order_delivered_carrier_date text,
    order_delivered_customer_date text,
    order_estimated_delivery_date text,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg.product_category_name_translation (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    product_category_name text NOT NULL,
    product_category_name_english text,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg.products (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    product_id text NOT NULL,
    product_category_name text,
    product_name_length real,
    product_description_length real,
    product_photos_qty real,
    product_weight_g real,
    product_length_cm real,
    product_height_cm real,
    product_width_cm real,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg.sellers (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    seller_id text NOT NULL,
    seller_zip_code_prefix integer,
    seller_city text,
    seller_state text,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE ONLY stg.geolocation
    ADD CONSTRAINT geolocation_pk UNIQUE (geolocation_zip_code_prefix);

ALTER TABLE ONLY stg.customers
    ADD CONSTRAINT pk_customers UNIQUE (customer_id);

ALTER TABLE ONLY stg.order_items
    ADD CONSTRAINT pk_order_items UNIQUE (order_id, order_item_id);

ALTER TABLE ONLY stg.order_payments
    ADD CONSTRAINT pk_order_payments UNIQUE (order_id, payment_sequential);

ALTER TABLE ONLY stg.order_reviews
    ADD CONSTRAINT pk_order_reviews UNIQUE (review_id, order_id);

ALTER TABLE ONLY stg.orders
    ADD CONSTRAINT pk_orders UNIQUE (order_id);

ALTER TABLE ONLY stg.product_category_name_translation
    ADD CONSTRAINT pk_product_category_name_translation UNIQUE (product_category_name);

ALTER TABLE ONLY stg.products
    ADD CONSTRAINT pk_products UNIQUE (product_id);

ALTER TABLE ONLY stg.sellers
    ADD CONSTRAINT pk_sellers UNIQUE (seller_id);

ALTER TABLE ONLY stg.customers
    ADD CONSTRAINT fk_cyst_geo_prefix FOREIGN KEY (customer_zip_code_prefix) REFERENCES stg.geolocation(geolocation_zip_code_prefix);

ALTER TABLE ONLY stg.order_items
    ADD CONSTRAINT fk_order_items_orders FOREIGN KEY (order_id) REFERENCES stg.orders(order_id);

ALTER TABLE ONLY stg.order_items
    ADD CONSTRAINT fk_order_items_products FOREIGN KEY (product_id) REFERENCES stg.products(product_id);

ALTER TABLE ONLY stg.order_items
    ADD CONSTRAINT fk_order_items_sellers FOREIGN KEY (seller_id) REFERENCES stg.sellers(seller_id);

ALTER TABLE ONLY stg.order_payments
    ADD CONSTRAINT fk_order_payments_orders FOREIGN KEY (order_id) REFERENCES stg.orders(order_id);

ALTER TABLE ONLY stg.order_reviews
    ADD CONSTRAINT fk_order_reviews_orders FOREIGN KEY (order_id) REFERENCES stg.orders(order_id);

ALTER TABLE ONLY stg.orders
    ADD CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) REFERENCES stg.customers(customer_id);

ALTER TABLE ONLY stg.products
    ADD CONSTRAINT fk_products_product_category FOREIGN KEY (product_category_name) REFERENCES stg.product_category_name_translation(product_category_name);

ALTER TABLE ONLY stg.sellers
    ADD CONSTRAINT fk_seller_geo_prefix FOREIGN KEY (seller_zip_code_prefix) REFERENCES stg.geolocation(geolocation_zip_code_prefix);