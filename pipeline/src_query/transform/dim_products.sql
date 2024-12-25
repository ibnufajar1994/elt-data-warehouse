-- CTE untuk mengambil dan menyiapkan data dari staging
WITH source AS (
    SELECT 
        id as product_id_sk,              -- UUID sebagai surrogate key
        product_id as product_id_nk,      -- Natural key dari source system
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    FROM stg.products
),

updated_records AS (
    UPDATE final.dim_products final
    SET current_flag = 'expired',
        updated_date = CURRENT_TIMESTAMP
    WHERE product_id_nk IN (
        -- Cari product yang ada perubahan data
        SELECT product_id_nk 
        FROM source src
        WHERE final.product_id_nk = src.product_id_nk
        AND (
            -- Deteksi perubahan pada kolom-kolom yang di-track
            final.product_category_name <> src.product_category_name
            OR final.product_name_length <> src.product_name_length
            OR final.product_description_length <> src.product_description_length
        
        )
    )
    AND current_flag = 'current'    -- Hanya update record yang masih current
    RETURNING *
)

INSERT INTO final.dim_products (
    product_id_sk,
    product_id_nk,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
    )

SELECT 
    product_id_sk,
    product_id_nk,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm

FROM source src


WHERE NOT EXISTS (
    -- Cek apakah data product sudah ada di dimension table dengan status current
    SELECT 1 
    FROM final.dim_products final 
    WHERE final.product_id_nk = src.product_id_nk
    AND final.current_flag = 'current'
)


OR 
    -- Kondisi 2: Insert untuk data yang berubah (versi baru)
    src.product_id_nk IN (
        SELECT product_id_nk 
        FROM updated_records
    );

