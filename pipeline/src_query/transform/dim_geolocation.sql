-- Transformation script for dim_geolocation

WITH stg_dim_geolocation AS (
    SELECT DISTINCT
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state
    FROM stg.geolocation
    WHERE geolocation_zip_code_prefix IS NOT NULL
)

-- Insert or update the geolocation dimension table
INSERT INTO final.dim_geolocation (
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    valid_from,
    valid_to,
    is_current
)
SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    CURRENT_TIMESTAMP AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM stg_dim_geolocation
ON CONFLICT (geolocation_zip_code_prefix)
DO UPDATE SET
    geolocation_lat = EXCLUDED.geolocation_lat,
    geolocation_lng = EXCLUDED.geolocation_lng,
    geolocation_city = EXCLUDED.geolocation_city,
    geolocation_state = EXCLUDED.geolocation_state,
    valid_from = CURRENT_TIMESTAMP,
    is_current = TRUE
WHERE (
    final.dim_geolocation.geolocation_lat != EXCLUDED.geolocation_lat OR
    final.dim_geolocation.geolocation_lng != EXCLUDED.geolocation_lng OR
    final.dim_geolocation.geolocation_city != EXCLUDED.geolocation_city OR
    final.dim_geolocation.geolocation_state != EXCLUDED.geolocation_state
);

-- Update the valid_to and is_current for old records
UPDATE final.dim_geolocation
SET valid_to = CURRENT_TIMESTAMP,
    is_current = FALSE
WHERE geolocation_zip_code_prefix IN (
    SELECT geolocation_zip_code_prefix
    FROM final.dim_geolocation
    WHERE is_current = TRUE
    GROUP BY geolocation_zip_code_prefix
    HAVING COUNT(*) > 1
)
AND is_current = TRUE
AND valid_from < (
    SELECT MAX(valid_from)
    FROM final.dim_geolocation AS inner_dim
    WHERE inner_dim.geolocation_zip_code_prefix = final.dim_geolocation.geolocation_zip_code_prefix
);