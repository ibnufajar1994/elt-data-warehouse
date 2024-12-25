WITH

    dim_customers AS (
        SELECT *
        FROM final.dim_customers
    ),

    stg_orders AS (
        SELECT 
        o.order_id,
        dc.customer_id_sk AS  customer_id_sk
        
        FROM stg.orders o 
        JOIN dim_customers dc 
        ON dc.customer_id_nk = o.customer_id

    ),

    final_fct_order_payments AS (
        SELECT
            sop.order_id as order_id_nk,
            so.customer_id_sk,
            sop.payment_sequential,
            sop.payment_type,
            sop.payment_installments,
            sop.payment_value,
            ROW_NUMBER() OVER (PARTITION BY sop.order_id ORDER BY sop.payment_sequential) as rn


        FROM
            stg.order_payments sop
        JOIN 
            stg_orders so
        ON  
            so.order_id = sop.order_id
            )

INSERT INTO final.fact_order_payments (
    order_id_nk,
    customer_id_sk,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value

)

SELECT
    order_id_nk,
    customer_id_sk,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value

FROM 
    final_fct_order_payments

WHERE 
    rn = 1

ON CONFLICT (order_id_nk)
DO UPDATE SET
    customer_id_sk = EXCLUDED.customer_id_sk,
    payment_sequential = EXCLUDED.payment_sequential,
    payment_type = EXCLUDED.payment_type,
    payment_installments = EXCLUDED.payment_installments,
    payment_value = EXCLUDED.payment_value,
    updated_date = CASE WHEN
                    final.fact_order_payments.customer_id_sk <> EXCLUDED.customer_id_sk
                    OR final.fact_order_payments.payment_sequential <> EXCLUDED.payment_sequential
                    OR final.fact_order_payments.payment_type <> EXCLUDED.payment_type
                    OR final.fact_order_payments.payment_installments <> EXCLUDED.payment_installments
                    OR final.fact_order_payments.payment_value <> EXCLUDED.payment_value
                    THEN CURRENT_TIMESTAMP
                    ELSE final.fact_order_payments.updated_date
                    END;
                    