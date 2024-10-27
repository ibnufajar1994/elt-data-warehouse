-- Transformation script for fact_order_items

INSERT INTO final.fact_order_items (
    order_key,
    product_key,
    seller_key,
    order_item_id,
    shipping_limit_date,
    price,
    freight_value
)
SELECT 
    fo.order_key,
    p.product_key,
    s.seller_key,
    oi.order_item_id,
    oi.shipping_limit_date::timestamp,
    oi.price::real,
    oi.freight_value::real
FROM stg.order_items oi
JOIN final.fact_orders fo ON oi.order_id = fo.order_id
JOIN final.dim_products p ON oi.product_id = p.product_id
JOIN final.dim_sellers s ON oi.seller_id = s.seller_id
ON CONFLICT (order_key, order_item_id) DO UPDATE SET
    product_key = EXCLUDED.product_key,
    seller_key = EXCLUDED.seller_key,
    shipping_limit_date = EXCLUDED.shipping_limit_date,
    price = EXCLUDED.price,
    freight_value = EXCLUDED.freight_value;