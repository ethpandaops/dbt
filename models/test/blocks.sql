{{ config(
    materialized="incremental",
    incremental_strategy="legacy",
    engine=var("replicated_merge_tree_engine_template"),
    order_by="(block)",
    unique_key="block",
    sharding_key="rand()",
    distributed=True,
) }}

WITH blocks AS (
    SELECT
        block,
        meta_network_name,
        count(*) AS total,
        min(slot_start_date_time) AS first_slot_start_date_time
    FROM
        {{ source('default', 'beacon_api_eth_v1_events_block') }}

    {% if is_incremental() %}
        WHERE slot_start_date_time > (
            SELECT max(first_slot_start_date_time) - INTERVAL '10 MINUTE'
            FROM {{ this }}
        )
    {% endif %}

    GROUP BY block, meta_network_name
)

SELECT *
FROM blocks

{% if is_incremental() %}

    WHERE first_slot_start_date_time > (
        SELECT max(first_slot_start_date_time) - INTERVAL '10 MINUTE'
        FROM {{ this }}
    )

{% endif %}
