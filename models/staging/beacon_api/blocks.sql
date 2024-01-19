{% set interval = '1 DAY' %}
{% set grace_period = '1 HOUR' %}
{% set current_time = run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}

{{
    config(
        materialized='distributed_incremental',
        engine="ReplicatedReplacingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}/{uuid}', '{replica}', updated_at)",
        incremental_strategy='append',
        order_by="(slot_started_at, unique_key, network)",
        unique_key="unique_key",
        sharding_key="unique_key",
        post_hook="INSERT INTO {{ target.schema }}.model_metadata (updated_date_time, model, last_run_date_time) SELECT NOW(), '{{ this }}', CASE WHEN MAX(last_run_date_time) = '1970-01-01 00:00:00' THEN parseDateTime64BestEffortOrNull('" ~ current_time ~ "') ELSE LEAST(MAX(last_run_date_time) + INTERVAL " ~ interval ~ ", parseDateTime64BestEffortOrNull('" ~ current_time ~ "')) END as end_time FROM {{ target.schema }}.model_metadata FINAL WHERE model = '{{ this }}'"
    )
}}

{% set run_times = check_model_metadata_run_times(this, "'" ~ current_time ~ "'", interval) %}

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
    WHERE
        slot_start_date_time BETWEEN '{{ run_times.start_time }}' - INTERVAL '{{ grace_period }}' AND '{{ run_times.end_time }}'

    GROUP BY unique_key, beacon_block_root_hash, slot, epoch, network
)

SELECT *
FROM blocks
