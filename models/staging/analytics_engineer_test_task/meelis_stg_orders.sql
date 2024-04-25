SELECT
    order_id,
    status,
    is_cancelled,
    customer_id,
    manufacturer_id,
    customer_price,
    manufacturer_price,
    shipping_price,
    markup,
    account_manager_country,
    created_at,
    in_production_at
FROM {{ source('analytics_engineer_test_task', 'orders') }}