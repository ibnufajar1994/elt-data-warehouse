CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Sources Schema
CREATE SCHEMA IF NOT EXISTS sources;

CREATE TABLE sources.customers (
    customer_id text NOT NULL,
    customer_unique_id text,
    customer_zip_code_prefix integer,
    customer_city text,
    customer_state text,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sources.geolocation (
    geolocation_zip_code_prefix integer NOT NULL,
    geolocation_lat real,
    geolocation_lng real,
    geolocation_city text,
    geolocation_state text,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sources.order_items (
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

CREATE TABLE sources.order_payments (
    order_id text NOT NULL,
    payment_sequential integer NOT NULL,
    payment_type text,
    payment_installments integer,
    payment_value real,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sources.order_reviews (
    review_id text NOT NULL,
    order_id text NOT NULL,
    review_score integer,
    review_comment_title text,
    review_comment_message text,
    review_creation_date text,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sources.orders (
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

CREATE TABLE sources.product_category_name_translation (
    product_category_name text NOT NULL,
    product_category_name_english text,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sources.products (
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

CREATE TABLE sources.sellers (
    seller_id text NOT NULL,
    seller_zip_code_prefix integer,
    seller_city text,
    seller_state text,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE ONLY sources.geolocation
    ADD CONSTRAINT geolocation_pk PRIMARY KEY (geolocation_zip_code_prefix);

ALTER TABLE ONLY sources.customers
    ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id);

ALTER TABLE ONLY sources.order_items
    ADD CONSTRAINT pk_order_items PRIMARY KEY (order_id, order_item_id);

ALTER TABLE ONLY sources.order_payments
    ADD CONSTRAINT pk_order_payments PRIMARY KEY (order_id, payment_sequential);

ALTER TABLE ONLY sources.order_reviews
    ADD CONSTRAINT pk_order_reviews PRIMARY KEY (review_id, order_id);

ALTER TABLE ONLY sources.orders
    ADD CONSTRAINT pk_orders PRIMARY KEY (order_id);

ALTER TABLE ONLY sources.product_category_name_translation
    ADD CONSTRAINT pk_product_category_name_translation PRIMARY KEY (product_category_name);

ALTER TABLE ONLY sources.products
    ADD CONSTRAINT pk_products PRIMARY KEY (product_id);

ALTER TABLE ONLY sources.sellers
    ADD CONSTRAINT pk_sellers PRIMARY KEY (seller_id);

ALTER TABLE ONLY sources.customers
    ADD CONSTRAINT fk_cyst_geo_prefix FOREIGN KEY (customer_zip_code_prefix) REFERENCES sources.geolocation(geolocation_zip_code_prefix);

ALTER TABLE ONLY sources.order_items
    ADD CONSTRAINT fk_order_items_orders FOREIGN KEY (order_id) REFERENCES sources.orders(order_id);

ALTER TABLE ONLY sources.order_items
    ADD CONSTRAINT fk_order_items_products FOREIGN KEY (product_id) REFERENCES sources.products(product_id);

ALTER TABLE ONLY sources.order_items
    ADD CONSTRAINT fk_order_items_sellers FOREIGN KEY (seller_id) REFERENCES sources.sellers(seller_id);

ALTER TABLE ONLY sources.order_payments
    ADD CONSTRAINT fk_order_payments_orders FOREIGN KEY (order_id) REFERENCES sources.orders(order_id);

ALTER TABLE ONLY sources.order_reviews
    ADD CONSTRAINT fk_order_reviews_orders FOREIGN KEY (order_id) REFERENCES sources.orders(order_id);

ALTER TABLE ONLY sources.orders
    ADD CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) REFERENCES sources.customers(customer_id);

ALTER TABLE ONLY sources.products
    ADD CONSTRAINT fk_products_product_category FOREIGN KEY (product_category_name) REFERENCES sources.product_category_name_translation(product_category_name);

ALTER TABLE ONLY sources.sellers
    ADD CONSTRAINT fk_seller_geo_prefix FOREIGN KEY (seller_zip_code_prefix) REFERENCES sources.geolocation(geolocation_zip_code_prefix);

