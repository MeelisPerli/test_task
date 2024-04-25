WITH

finish AS (
    SELECT DISTINCT
        order_part_id,
        CASE WHEN process_name LIKE 'SURFACE_FINISH' THEN REPLACE(JSON_EXTRACT(process_config, '$.value'), '"', '') END AS surface_finish,
        CASE WHEN process_name LIKE 'SECONDARY_SURFACE_FINISH' THEN REPLACE(JSON_EXTRACT(process_config, '$.value'), '"', '') END AS secondary_surface_finish,
        CASE WHEN process_name LIKE 'SECONDARY_SURFACE_FINISH_RAL' THEN REPLACE(JSON_EXTRACT(process_config, '$.ralCode') ,'"', '') END AS ral_code,
        CASE WHEN process_name LIKE 'SECONDARY_SURFACE_FINISH_RAL' THEN REPLACE(JSON_EXTRACT(process_config, '$.ralFinish'), '"', '') END AS ral_finish
    FROM {{ ref('meelis_stg_parts_surface_finish_config') }}
),

final AS (
    SELECT
        parts.order_part_id,
        parts.order_id,
        parts.selected_process_type,
        parts.material_name,
        parts.material_type,
        parts.weight_g,
        parts.quantity,
        parts.manufacturer_price_eur,
        parts.has_bending,
        parts.has_surface_coating,
        parts.has_insert_operations,
        parts.bends_count,
        parts.created_at,
        finish.surface_finish,
        finish.secondary_surface_finish,
        finish.ral_code,
        finish.ral_finish
    FROM {{ ref('meelis_stg_parts') }} AS parts
    LEFT JOIN finish ON parts.order_part_id = finish.order_part_id
)

SELECT *, CURRENT_TIMESTAMP() AS run_time  FROM final