WITH source AS (
    SELECT 
        id as seller_id_sk,              -- UUID sebagai surrogate key
        seller_id as seller_id_nk,      -- Natural key dari source system
        seller_zip_code_prefix,
        seller_city,
        seller_state
    FROM stg.sellers
),

updated_records AS (
    UPDATE final.dim_sellers final
    SET current_flag = 'expired',
        updated_date = CURRENT_TIMESTAMP
    WHERE seller_id_nk IN (
        -- Cari product yang ada perubahan data
        SELECT seller_id_nk 
        FROM source src
        WHERE final.seller_id_nk = src.seller_id_nk
        AND (
            -- Deteksi perubahan pada kolom-kolom yang di-track
            final.seller_zip_code_prefix <> src.seller_zip_code_prefix
            OR final.seller_city<> src.seller_city
            OR final.seller_state <> src.seller_state
        
        )
    )
    AND current_flag = 'current'    -- Hanya update record yang masih current
    RETURNING *
)

INSERT INTO final.dim_sellers (
    seller_id_sk,
    seller_id_nk,
    seller_zip_code_prefix,
    seller_city,
    seller_state
    )

SELECT 
    seller_id_sk,
    seller_id_nk,
    seller_zip_code_prefix,
    seller_city,
    seller_state

FROM source src


WHERE NOT EXISTS (
    -- Cek apakah data seller sudah ada di dimension table dengan status current
    SELECT 1 
    FROM final.dim_sellers final 
    WHERE final.seller_id_nk = src.seller_id_nk
    AND final.current_flag = 'current'
)

OR 
    -- Kondisi 2: Insert untuk data yang berubah (versi baru)
    src.seller_id_nk IN (
        SELECT seller_id_nk 
        FROM updated_records
    );

