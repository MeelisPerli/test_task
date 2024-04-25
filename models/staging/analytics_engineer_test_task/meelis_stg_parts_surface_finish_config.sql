SELECT
    order_part_id,
    process_name,
    process_config
FROM {{ source('analytics_engineer_test_task', 'parts_surface_finish_config') }}