WITH stg_orders AS (
    SELECT 
        so1.order_id as order_id_nk,
        so2.product_id,
        so2.seller_id,
        so2.price,
        so2.freight_value,
        so1.customer_id,
        dd1.date_id as date_id,
        ROW_NUMBER() OVER (PARTITION BY so1.order_id) as rn

    FROM stg.orders so1
    JOIN stg.order_items so2
        ON so2.order_id = so1.order_id
    JOIN final.dim_date dd1
        ON dd1.date_actual = DATE(so1.order_purchase_timestamp)
),
dim_date AS (
    SELECT *
    FROM final.dim_date
),
dim_products AS (
    SELECT *
    FROM final.dim_products
),
dim_customers AS (
    SELECT *
    FROM final.dim_customers
),
dim_sellers AS (
    SELECT *
    FROM final.dim_sellers
),
final_fct_order_items AS (
    SELECT 
        sto.order_id_nk as order_id_nk,
        sto.date_id,
        dp.product_id_sk,
        dc.customer_id_sk,
        ds.seller_id_sk,
        sto.price,
        sto.freight_value
    FROM stg_orders sto
    JOIN dim_products dp
        ON dp.product_id_nk = sto.product_id
    JOIN dim_customers dc
        ON dc.customer_id_nk = sto.customer_id
    JOIN dim_sellers ds
        ON ds.seller_id_nk = sto.seller_id
    WHERE sto.rn = 1
)
INSERT INTO final.fact_order_line_items (
    order_id_nk,
    date_id,
    product_id_sk,
    customer_id_sk,
    seller_id_sk,
    price,
    freight_value
)
SELECT 
    order_id_nk,
    date_id,
    product_id_sk,
    customer_id_sk,
    seller_id_sk,
    price,
    freight_value
FROM final_fct_order_items

ON CONFLICT(order_id_nk)
DO UPDATE SET
    date_id = EXCLUDED.date_id,
    product_id_sk = EXCLUDED.product_id_sk,
    customer_id_sk = EXCLUDED.customer_id_sk,
    seller_id_sk = EXCLUDED.seller_id_sk,
    price = EXCLUDED.price,
    freight_value = EXCLUDED.freight_value,
    updated_date = CASE 
        WHEN final.fact_order_line_items.date_id <> EXCLUDED.date_id
            OR final.fact_order_line_items.product_id_sk <> EXCLUDED.product_id_sk
            OR final.fact_order_line_items.customer_id_sk <> EXCLUDED.customer_id_sk
            OR final.fact_order_line_items.seller_id_sk <> EXCLUDED.seller_id_sk
            OR final.fact_order_line_items.price <> EXCLUDED.price
            OR final.fact_order_line_items.freight_value <> EXCLUDED.freight_value
        THEN CURRENT_TIMESTAMP
        ELSE final.fact_order_line_items.updated_date
    END;