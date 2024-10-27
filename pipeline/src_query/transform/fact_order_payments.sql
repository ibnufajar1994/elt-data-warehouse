-- Transformation script for fact_order_payments

INSERT INTO final.fact_order_payments (
    order_key,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
)
SELECT 
    fo.order_key,
    op.payment_sequential,
    op.payment_type,
    op.payment_installments,
    op.payment_value::real
FROM stg.order_payments op
JOIN final.fact_orders fo ON op.order_id = fo.order_id
ON CONFLICT (order_key, payment_sequential) DO UPDATE SET
    payment_type = EXCLUDED.payment_type,
    payment_installments = EXCLUDED.payment_installments,
    payment_value = EXCLUDED.payment_value;

