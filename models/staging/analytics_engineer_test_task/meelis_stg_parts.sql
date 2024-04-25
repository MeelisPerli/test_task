SELECT
    order_part_id,
    order_id,
    --file_name,
    selected_process_type,
    material_name,
    material_type,
    weight_g,
    quantity,
    manufacturer_price_eur,
    has_bending,
    has_surface_coating,
    has_insert_operations,
    bends_count,
    created_at
FROM {{ source('analytics_engineer_test_task', 'parts') }}