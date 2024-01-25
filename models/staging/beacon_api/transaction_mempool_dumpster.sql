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
        `from`,
        `to`,
        value,
        nonce,
        gas,
        gas_price,
        gas_tip_cap,
        gas_fee_cap,
        data_size,
        data_4bytes,
        sources,
        included_at_block_height,
        toDateTime(included_block_timestamp) as included_block_timestamp,
        inclusion_delay_ms
    FROM {{ source('clickhouse', 'mempool_dumpster_transaction') }}
    WHERE toDateTime(timestamp) BETWEEN '{{ run_times.start_time }}' AND '{{ run_times.end_time }}'
        AND chain_id = '1'
    GROUP BY 
        hash,
        from,
        to,
        value,
        nonce,
        gas,
        gas_price,
        gas_tip_cap,
        gas_fee_cap,
        data_size,
        data_4bytes,
        sources,
        included_at_block_height,
        included_block_timestamp,
        inclusion_delay_ms
)

SELECT *, 'mainnet' as network
FROM transactions
