WITH source AS (
    SELECT 
        id as geolocation_sk,              -- UUID sebagai surrogate key
        geolocation_zip_code_prefix,      -- Natural key dari source system
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state
    FROM stg.geolocation
),

updated_records AS (
    UPDATE final.dim_geolocation final
    SET current_flag = 'expired',
        updated_date = CURRENT_TIMESTAMP
    WHERE geolocation_zip_code_prefix IN (
        -- Cari product yang ada perubahan data
        SELECT geolocation_zip_code_prefix 
        FROM source src
        WHERE final.geolocation_zip_code_prefix = src.geolocation_zip_code_prefix
        AND (
            -- Deteksi perubahan pada kolom-kolom yang di-track
            final.geolocation_city <> src.geolocation_city 
            OR final.geolocation_state <> src.geolocation_state
        
        )
    )
    AND current_flag = 'current'    -- Hanya update record yang masih current
    RETURNING *
)

INSERT INTO final.dim_geolocation (
    geolocation_sk,
    geolocation_zip_code_prefix,      
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
    )

SELECT 
    geolocation_sk,
    geolocation_zip_code_prefix,      
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state

FROM source src


WHERE NOT EXISTS (
    -- Cek apakah data sudah ada di dimension table dengan status current
    SELECT 1 
    FROM final.dim_geolocation final 
    WHERE final.geolocation_zip_code_prefix = src.geolocation_zip_code_prefix
    AND final.current_flag = 'current'
)

OR 
    -- Kondisi 2: Insert untuk data yang berubah (versi baru)
    src.geolocation_zip_code_prefix IN (
        SELECT geolocation_zip_code_prefix
        FROM updated_records
    );
