{% set interval = '1 DAY' %}
{% set grace_period = '1 HOUR' %}
{% set current_time = run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}

{{
    config(
        materialized='distributed_incremental',
        engine="ReplicatedReplacingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}/{uuid}', '{replica}', updated_at)",
        incremental_strategy='append',
        order_by="(hash, network)",
        unique_key="hash",
        post_hook="INSERT INTO {{ target.schema }}.model_metadata (unique_key, updated_date_time, model, last_run_date_time) SELECT cityHash64('{{ this }}'), NOW(), '{{ this }}', CASE WHEN MAX(last_run_date_time) = '1970-01-01 00:00:00' THEN parseDateTime64BestEffortOrNull('" ~ current_time ~ "') ELSE LEAST(MAX(last_run_date_time) + INTERVAL " ~ interval ~ ", parseDateTime64BestEffortOrNull('" ~ current_time ~ "')) END as end_time FROM {{ target.schema }}.model_metadata FINAL WHERE model = '{{ this }}'"
    )
}}

{% set run_times = check_model_metadata_run_times(this, "'" ~ current_time ~ "'", interval) %}

WITH hashes AS (
    SELECT
        NOW() AS updated_at,
        'mainnet' as network,
        hash
    FROM (
        SELECT hash FROM {{ source('clickhouse', 'mempool_transaction') }}
        WHERE event_date_time BETWEEN '{{ run_times.start_time }}' AND '{{ run_times.end_time }}' 
        AND meta_network_name = 'mainnet'

        UNION ALL

        SELECT hash FROM {{ source('clickhouse', 'block_native_mempool_transaction') }}
        WHERE detecttime BETWEEN '{{ run_times.start_time }}' AND '{{ run_times.end_time }}' 
        AND network = 'main'

        UNION ALL

        SELECT hash FROM {{ source('clickhouse', 'mempool_dumpster_transaction') }}
        WHERE timestamp BETWEEN '{{ run_times.start_time }}' AND '{{ run_times.end_time }}' 
        AND chain_id = '1'

        UNION ALL

        SELECT hash FROM {{ source('clickhouse', 'canonical_beacon_block_execution_transaction') }}
        WHERE slot_start_date_time BETWEEN '{{ run_times.start_time }}' AND '{{ run_times.end_time }}' 
        AND meta_network_name = 'mainnet'
    ) 
    GROUP BY hash
)

SELECT *
FROM hashes
