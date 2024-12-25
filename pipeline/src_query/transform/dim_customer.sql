-- CTE untuk mengambil dan menyiapkan data dari staging
WITH source AS (
    SELECT 
        id as customer_id_sk,              -- UUID sebagai surrogate key
        customer_id as customer_id_nk,      -- Natural key dari source system
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM stg.customers
),

-- Update records yang berubah dengan mengubah current_flag menjadi 'expired'
updated_records AS (
    UPDATE final.dim_customers final
    SET current_flag = 'expired',
        updated_date = CURRENT_TIMESTAMP
    WHERE customer_id_nk IN (
        -- Cari customer yang ada perubahan data
        SELECT customer_id_nk 
        FROM source src
        WHERE final.customer_id_nk = src.customer_id_nk
        AND (
            -- Deteksi perubahan pada kolom-kolom yang di-track
            final.customer_zip_code_prefix <> src.customer_zip_code_prefix 
            OR final.customer_city <> src.customer_city
            OR final.customer_state <> src.customer_state
        )
    )
    AND current_flag = 'current'    -- Hanya update record yang masih current
    RETURNING *
)

-- Insert data baru:
-- 1. Record yang belum ada di dimension table
-- 2. Version baru dari record yang berubah (yang sudah di-expire di atas)
INSERT INTO final.dim_customers (
    customer_id_sk,
    customer_id_nk,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
)
SELECT 
    customer_id_sk,
    customer_id_nk,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM source src
WHERE NOT EXISTS (
    -- Cek apakah customer sudah ada di dimension table dengan status current
    SELECT 1 
    FROM final.dim_customers final 
    WHERE final.customer_id_nk = src.customer_id_nk
    AND final.current_flag = 'current'
)

OR 
    -- Kondisi 2: Insert untuk data yang berubah (versi baru)
    src.customer_id_nk IN (
        SELECT customer_id_nk 
        FROM updated_records
    );

