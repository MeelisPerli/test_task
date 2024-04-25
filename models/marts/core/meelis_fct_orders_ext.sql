WITH
parts AS (
    SELECT
        order_id,
        count(*) AS parts_count,
        countif(selected_process_type like 'cnc_machining') AS cnc_part_count,
        countif(selected_process_type like '%laser%') AS laser_part_count,
        countif(has_bending = 1) AS bending_part_count,
        sum(COALESCE(bends_count, 0)) AS total_bends_count,
        countif(has_surface_coating) AS surface_coating_part_count,
        countif(has_insert_operations) AS insert_operations_part_count,
        STRING_AGG(DISTINCT ral_code, ',') AS ral_codes,
        STRING_AGG(DISTINCT ral_finish, ',') AS ral_finishes,
        STRING_AGG(DISTINCT surface_finish, ',') AS surface_finishes,
        STRING_AGG(DISTINCT secondary_surface_finish, ',') AS secondary_surface_finishes
    FROM {{ ref('meelis_fct_parts_ext') }}
    GROUP BY order_id
),

final AS (
    SELECT
        orders.order_id,
        orders.status,
        orders.is_cancelled,
        orders.customer_id,
        orders.manufacturer_id,
        orders.customer_price,
        orders.manufacturer_price,
        orders.shipping_price,
        orders.markup,
        orders.account_manager_country,
        orders.created_at,
        orders.in_production_at,
        parts.parts_count,
        parts.cnc_part_count,
        parts.laser_part_count,
        parts.bending_part_count,
        parts.total_bends_count,
        parts.surface_coating_part_count,
        parts.insert_operations_part_count,
        parts.ral_codes,
        parts.ral_finishes,
        parts.surface_finishes,
        parts.secondary_surface_finishes
    FROM {{ ref('meelis_stg_orders') }} AS orders
    LEFT JOIN parts ON orders.order_id = parts.order_id
)

SELECT *, CURRENT_TIMESTAMP() AS run_time FROM final