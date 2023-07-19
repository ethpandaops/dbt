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
        count(*) AS total,
        min(epoch) AS first_epoch,
        meta_network_name
    FROM
        {{ source('default', 'beacon_api_eth_v1_events_block') }}
    GROUP BY block, meta_network_name
)

SELECT *
FROM blocks

{% if is_incremental() %}

    WHERE first_epoch > (SELECT max(first_epoch) - 2 FROM {{ this }})

{% endif %}
