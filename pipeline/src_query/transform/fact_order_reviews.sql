
WITH 

    dim_customers AS (
    SELECT *
    FROM final.dim_customers
    ),


    dim_sellers AS (
        SELECT 
            *
        FROM
            final.dim_sellers
    ),

    dim_reviews AS (
        SELECT *
        FROM final.dim_reviews
    ),

    stg_order_items AS (
        SELECT 
            DISTINCT ds.seller_id_sk,
            soi.order_id as order_id
        FROM 
            stg.order_items soi
        JOIN 
            dim_sellers ds
        ON 
            ds.seller_id_nk = soi.seller_id

    ),

    stg_orders AS (
    SELECT 
        DISTINCT dc.customer_id_sk as customer_id_sk,
        so.order_id as order_id

    FROM 
        stg.orders so
    JOIN 
        dim_customers dc
    ON
        dc.customer_id_nk = so.customer_id
    
    ),


    stg_order_reviews AS (
        SELECT 
            DISTINCT
        sor.order_id as order_id,
        sor.review_id,
        review_score
        FROM stg.order_reviews sor
    ),

    final_fact_order_reviews AS (
        SELECT *
        FROM (
            SELECT 
                dr.review_id_sk,
                dr.review_id_nk,
                sor.order_id as order_id_nk,
                so.customer_id_sk,
                soi.seller_id_sk,
                sor.review_score,
                ROW_NUMBER() OVER (PARTITION BY dr.review_id_nk ORDER BY sor.order_id) as rn
            FROM dim_reviews dr
            JOIN stg_order_items soi ON soi.order_id = dr.order_id_nk
            JOIN stg_orders so ON so.order_id = dr.order_id_nk
            JOIN stg_order_reviews sor ON sor.review_id = dr.review_id_nk
        ) ranked
        WHERE rn = 1
    )

INSERT INTO final.fact_order_reviews (
    order_id_nk,
    review_id_sk,
    review_id_nk,
    seller_id_sk,
    customer_id_sk,
    review_score



)

SELECT
DISTINCT
    order_id_nk,
    review_id_sk,
    review_id_nk,
    seller_id_sk,
    customer_id_sk,
    review_score
FROM 
    final_fact_order_reviews

ON CONFLICT(review_id_nk)
DO UPDATE SET
    order_id_nk = EXCLUDED.order_id_nk,
    review_id_sk = EXCLUDED.review_id_sk,
    seller_id_sk = EXCLUDED.seller_id_sk,
    customer_id_sk = EXCLUDED.customer_id_sk,
    review_score = EXCLUDED.review_score,
    updated_date = CASE WHEN
                        final.fact_order_reviews.order_id_nk <> EXCLUDED.order_id_nk
                        OR final.fact_order_reviews.review_id_sk <> EXCLUDED.review_id_sk
                        OR final.fact_order_reviews.seller_id_sk <> EXCLUDED.seller_id_sk
                        OR final.fact_order_reviews.customer_id_sk <> EXCLUDED.customer_id_sk
                        OR final.fact_order_reviews.review_score <> EXCLUDED.review_score
                        THEN
                            CURRENT_TIMESTAMP
                        ELSE    
                            final.fact_order_reviews.updated_date
                        END;

