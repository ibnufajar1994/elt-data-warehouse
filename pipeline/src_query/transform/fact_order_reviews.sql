INSERT INTO final.fact_order_reviews (
    order_key,
    review_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date
)
SELECT 
    fo.order_key,
    orv.review_id,
    orv.review_score::integer,
    orv.review_comment_title,
    orv.review_comment_message,
    orv.review_creation_date::timestamp
FROM stg.order_reviews orv
JOIN final.fact_orders fo ON orv.order_id = fo.order_id
ON CONFLICT (order_key, review_id) DO UPDATE SET
    review_score = EXCLUDED.review_score,
    review_comment_title = EXCLUDED.review_comment_title,
    review_comment_message = EXCLUDED.review_comment_message,
    review_creation_date = EXCLUDED.review_creation_date;