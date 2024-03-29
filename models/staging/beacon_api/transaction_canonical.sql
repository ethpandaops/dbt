{% set interval = '1 DAY' %}
{% set grace_period = '6 HOUR' %}
{% set current_time = run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}

{{
    config(
        materialized='distributed_incremental',
        engine="ReplicatedReplacingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}/{uuid}', '{replica}', updated_at)",
        incremental_strategy='append',
        order_by="(hash, network)",
        unique_key="hash",
        sharding_key="cityHash64(hash)",
        post_hook="INSERT INTO {{ target.schema }}.model_metadata (model, updated_date_time, last_run_date_time) SELECT '{{ this }}', NOW(), CASE WHEN MAX(last_run_date_time) = '1970-01-01 00:00:00' THEN parseDateTime64BestEffortOrNull('" ~ current_time ~ "') ELSE LEAST(MAX(last_run_date_time) + INTERVAL " ~ interval ~ ", parseDateTime64BestEffortOrNull('" ~ current_time ~ "')) END as end_time FROM {{ target.schema }}.model_metadata FINAL WHERE model = '{{ this }}'"
    )
}}

{% set run_times = check_model_metadata_run_times(this, "'" ~ current_time ~ "'", interval) %}

WITH transactions AS (
    SELECT
        hash,
        NOW() as updated_at,
        meta_network_name as network,
        slot,
        slot_start_date_time as slot_started_at,
        epoch,
        epoch_start_date_time as epoch_start_at,
        block_root,
        block_version,
        position,
        `from`,
        `to`,
        nonce,
        gas_price,
        gas,
        value,
        type,
        size,
        call_data_size
    FROM {{ source('clickhouse', 'canonical_beacon_block_execution_transaction') }}
    WHERE slot_start_date_time BETWEEN '{{ run_times.start_time }}' AND '{{ run_times.end_time }}'
        AND meta_network_name = 'mainnet'
    GROUP BY 
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        block_version,
        position,
        hash,
        from,
        to,
        nonce,
        gas_price,
        gas,
        value,
        type,
        size,
        call_data_size,
        meta_network_name
)

SELECT *
FROM transactions
