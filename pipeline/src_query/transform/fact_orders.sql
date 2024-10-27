-- Transformation script for fact_orders

INSERT INTO final.fact_orders (
    order_id,
    customer_key,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
)
SELECT 
    o.order_id,
    c.customer_key,
    o.order_status,
    o.order_purchase_timestamp::timestamp,
    o.order_approved_at::timestamp,
    o.order_delivered_carrier_date::timestamp,
    o.order_delivered_customer_date::timestamp,
    o.order_estimated_delivery_date::timestamp
FROM stg.orders o
JOIN final.dim_customer c ON o.customer_id = c.customer_id
ON CONFLICT (order_id) DO UPDATE SET
    customer_key = EXCLUDED.customer_key,
    order_status = EXCLUDED.order_status,
    order_purchase_timestamp = EXCLUDED.order_purchase_timestamp,
    order_approved_at = EXCLUDED.order_approved_at,
    order_delivered_carrier_date = EXCLUDED.order_delivered_carrier_date,
    order_delivered_customer_date = EXCLUDED.order_delivered_customer_date,
    order_estimated_delivery_date = EXCLUDED.order_estimated_delivery_date;

