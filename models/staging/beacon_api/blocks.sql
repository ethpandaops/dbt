{{ config(
    materialized="incremental",
    incremental_strategy="append",
    engine=" ReplicatedReplacingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}/{uuid}', '{replica}', updated_at)",
    order_by="(unique_key)",
    unique_key="unique_key",
    sharding_key="unique_key",
    distributed=True,
) }}

WITH blocks AS (
    SELECT
        slot,
        epoch,
        block AS beacon_block_root_hash,
        meta_network_name AS network,
        xxHash32(block) AS unique_key, -- noqa: CP03
        NOW() AS updated_at,
        MIN(slot_start_date_time) AS slot_started_at,
        COUNT(*) AS total_witnesses
    FROM
        {{ source('clickhouse', 'beacon_api_eth_v1_events_block') }}

    {% if is_incremental() %}
        WHERE slot_start_date_time >= (
            SELECT MAX(slot_started_at) - INTERVAL '1 MINUTE'
            FROM {{ this }}
        )
    {% endif %}

    GROUP BY unique_key, beacon_block_root_hash, slot, epoch, network
)

SELECT *
FROM blocks

{% if is_incremental() %}

    WHERE slot_started_at >= (
        SELECT MAX(slot_started_at) - INTERVAL '1 MINUTE'
        FROM {{ this }}
    )

{% endif %}
