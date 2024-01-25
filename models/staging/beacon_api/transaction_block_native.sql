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
        nonce,
        gas,
        gasprice as gas_price,
        value,
        toaddress as `to`,
        fromaddress as `from`,
        datasize as call_data_size,
        data4bytes as call_data_4_bytes,
        type,
        groupUniqArray(status) AS statuses,
        min(replace) as replace,
        min(reorg) as reorg,
        min(blockspending) as blocks_pending,
        min(maxpriorityfeepergas) as max_priority_fee_per_gas,
        min(maxfeepergas) as max_fee_per_gas,
        groupUniqArray(basefeepergas) AS base_fee_per_gas,
        min(gasused) as gas_used,
        groupUniqArray(failurereason) AS failure_reasons,
        groupUniqArray(dropreason) AS drop_reasons,
        groupUniqArray(rejectionreason) AS rejection_reasons,
        groupUniqArray(region) AS seen_regions,
        min(detecttime) AS first_detected_at,
        min(timepending) AS first_time_pending_at
    FROM {{ source('clickhouse', 'block_native_mempool_transaction') }}
    WHERE detecttime BETWEEN '{{ run_times.start_time }}' AND '{{ run_times.end_time }}'
        AND network = 'main'
    GROUP BY 
        hash,
        nonce,
        gas,
        gasprice,
        value,
        toaddress,
        fromaddress,
        datasize,
        data4bytes,
        type
)

SELECT *, 'mainnet' as network
FROM transactions
