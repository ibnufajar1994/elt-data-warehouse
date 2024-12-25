WITH
    stg_order_reviews AS (
        SELECT *,
                ROW_NUMBER() OVER (PARTITION BY sto.review_id) as rn
            FROM stg.order_reviews sto
    )


INSERT INTO final.dim_reviews (
    review_id_sk,
    review_id_nk,
    order_id_nk,
    review_comment_title,
    review_comment_message
)



SELECT
    storv.id as review_id_sk,
   storv.review_id as review_id_nk,
    storv.order_id as order_id_nk,
    storv.review_comment_title,
    storv.review_comment_message

FROM 
    stg_order_reviews storv

WHERE 
    storv.rn = 1

ON CONFLICT (review_id_nk)
DO UPDATE SET
    review_id_sk = EXCLUDED.review_id_sk,
    order_id_nk = EXCLUDED.order_id_nk,
    review_comment_title = EXCLUDED.review_comment_title,
    review_comment_message = EXCLUDED.review_comment_message,
    updated_date = CASE WHEN
                        final.dim_reviews.review_id_sk <> EXCLUDED.review_id_sk
                        OR final.dim_reviews.order_id_nk <> EXCLUDED.order_id_nk
                        OR final.dim_reviews.review_comment_title <> EXCLUDED.review_comment_title
                        OR final.dim_reviews.review_comment_message <> EXCLUDED.review_comment_message
                
                 THEN
                        CURRENT_TIMESTAMP
                 ELSE
                        final.dim_reviews.updated_date
                 END;