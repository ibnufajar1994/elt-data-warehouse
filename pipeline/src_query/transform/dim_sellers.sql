-- Transformation script for dim_sellers

WITH stg_dim_sellers AS (
    SELECT DISTINCT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    FROM stg.sellers
    WHERE seller_id IS NOT NULL
)

-- Insert or update the sellers dimension table
INSERT INTO final.dim_sellers (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    valid_from,
    valid_to,
    is_current
)
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    CURRENT_TIMESTAMP AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM stg_dim_sellers
ON CONFLICT (seller_id)
DO UPDATE SET
    seller_zip_code_prefix = EXCLUDED.seller_zip_code_prefix,
    seller_city = EXCLUDED.seller_city,
    seller_state = EXCLUDED.seller_state,
    valid_from = CURRENT_TIMESTAMP,
    is_current = TRUE
WHERE (
    final.dim_sellers.seller_zip_code_prefix != EXCLUDED.seller_zip_code_prefix OR
    final.dim_sellers.seller_city != EXCLUDED.seller_city OR
    final.dim_sellers.seller_state != EXCLUDED.seller_state
);

-- Update the valid_to and is_current for old records
UPDATE final.dim_sellers
SET valid_to = CURRENT_TIMESTAMP,
    is_current = FALSE
WHERE seller_id IN (
    SELECT seller_id
    FROM final.dim_sellers
    WHERE is_current = TRUE
    GROUP BY seller_id
    HAVING COUNT(*) > 1
)
AND is_current = TRUE
AND valid_from < (
    SELECT MAX(valid_from)
    FROM final.dim_sellers AS inner_dim
    WHERE inner_dim.seller_id = final.dim_sellers.seller_id
);