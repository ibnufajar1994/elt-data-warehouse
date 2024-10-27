-- Transformation script for dim_customer

WITH stg_dim_customer AS (
    SELECT DISTINCT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM stg.customers
    WHERE customer_id IS NOT NULL
)

-- Insert or update the customer dimension table
INSERT INTO final.dim_customer (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    valid_from,
    valid_to,
    is_current
)
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    CURRENT_TIMESTAMP AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM stg_dim_customer
ON CONFLICT (customer_id)
DO UPDATE SET
    customer_unique_id = EXCLUDED.customer_unique_id,
    customer_zip_code_prefix = EXCLUDED.customer_zip_code_prefix,
    customer_city = EXCLUDED.customer_city,
    customer_state = EXCLUDED.customer_state,
    valid_from = CURRENT_TIMESTAMP,
    is_current = TRUE
WHERE (
    final.dim_customer.customer_unique_id != EXCLUDED.customer_unique_id OR
    final.dim_customer.customer_zip_code_prefix != EXCLUDED.customer_zip_code_prefix OR
    final.dim_customer.customer_city != EXCLUDED.customer_city OR
    final.dim_customer.customer_state != EXCLUDED.customer_state
);

-- Update the valid_to and is_current for old records
UPDATE final.dim_customer
SET valid_to = CURRENT_TIMESTAMP,
    is_current = FALSE
WHERE customer_id IN (
    SELECT customer_id
    FROM final.dim_customer
    WHERE is_current = TRUE
    GROUP BY customer_id
    HAVING COUNT(*) > 1
)
AND is_current = TRUE
AND valid_from < (
    SELECT MAX(valid_from)
    FROM final.dim_customer AS inner_dim
    WHERE inner_dim.customer_id = final.dim_customer.customer_id
);