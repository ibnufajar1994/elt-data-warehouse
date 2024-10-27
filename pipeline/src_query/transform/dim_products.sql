-- Transformation script for dim_products

WITH stg_dim_products AS (
    SELECT DISTINCT
        p.product_id,
        p.product_category_name,
        pn.product_category_name_english,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm
    FROM stg.products p
    LEFT JOIN stg.product_category_name_translation pn ON p.product_category_name = pn.product_category_name
    WHERE p.product_id IS NOT NULL
)

-- Insert or update the products dimension table
INSERT INTO final.dim_products (
    product_id,
    product_category_name,
    product_category_name_english,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    valid_from,
    valid_to,
    is_current
)
SELECT
    product_id,
    product_category_name,
    product_category_name_english,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    CURRENT_TIMESTAMP AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM stg_dim_products
ON CONFLICT (product_id)
DO UPDATE SET
    product_category_name = EXCLUDED.product_category_name,
    product_category_name_english = EXCLUDED.product_category_name_english,
    product_name_length = EXCLUDED.product_name_length,
    product_description_length = EXCLUDED.product_description_length,
    product_photos_qty = EXCLUDED.product_photos_qty,
    product_weight_g = EXCLUDED.product_weight_g,
    product_length_cm = EXCLUDED.product_length_cm,
    product_height_cm = EXCLUDED.product_height_cm,
    product_width_cm = EXCLUDED.product_width_cm,
    valid_from = CURRENT_TIMESTAMP,
    is_current = TRUE
WHERE (
    final.dim_products.product_category_name != EXCLUDED.product_category_name OR
    final.dim_products.product_category_name_english != EXCLUDED.product_category_name_english OR
    final.dim_products.product_name_length != EXCLUDED.product_name_length OR
    final.dim_products.product_description_length != EXCLUDED.product_description_length OR
    final.dim_products.product_photos_qty != EXCLUDED.product_photos_qty OR
    final.dim_products.product_weight_g != EXCLUDED.product_weight_g OR
    final.dim_products.product_length_cm != EXCLUDED.product_length_cm OR
    final.dim_products.product_height_cm != EXCLUDED.product_height_cm OR
    final.dim_products.product_width_cm != EXCLUDED.product_width_cm
);

-- Update the valid_to and is_current for old records
UPDATE final.dim_products
SET valid_to = CURRENT_TIMESTAMP,
    is_current = FALSE
WHERE product_id IN (
    SELECT product_id
    FROM final.dim_products
    WHERE is_current = TRUE
    GROUP BY product_id
    HAVING COUNT(*) > 1
)
AND is_current = TRUE
AND valid_from < (
    SELECT MAX(valid_from)
    FROM final.dim_products AS inner_dim
    WHERE inner_dim.product_id = final.dim_products.product_id
);