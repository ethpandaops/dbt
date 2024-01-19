{% set interval = '1 HOUR' %}
{% set grace_period = '5 MINUTE' %}
{% set current_time = run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}

{{
    config(
        materialized='distributed_incremental',
        engine="ReplicatedReplacingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}/{uuid}', '{replica}', updated_at)",
        incremental_strategy='append',
        order_by="(slot_started_at, unique_key, network)",
        unique_key="unique_key",
        sharding_key="unique_key",
        post_hook="INSERT INTO {{ target.schema }}.model_metadata (unique_key, updated_date_time, model, last_run_date_time) SELECT cityHash64('{{ this }}'), NOW(), '{{ this }}', CASE WHEN MAX(last_run_date_time) = '1970-01-01 00:00:00' THEN parseDateTime64BestEffortOrNull('" ~ current_time ~ "') ELSE LEAST(MAX(last_run_date_time) + INTERVAL " ~ interval ~ ", parseDateTime64BestEffortOrNull('" ~ current_time ~ "')) END as end_time FROM {{ target.schema }}.model_metadata FINAL WHERE model = '{{ this }}'"
    )
}}

{% set run_times = check_model_metadata_run_times(this, "'" ~ current_time ~ "'", interval) %}

WITH ranked_data AS (
    SELECT
        slot,
        slot_start_date_time,
        attesting_validator_index,
        meta_network_name,
        meta_client_geo_country,
        meta_client_geo_continent_code,
        propagation_slot_start_diff,
        ROW_NUMBER() OVER (
            PARTITION BY
                slot_start_date_time,
                attesting_validator_index
            ORDER BY propagation_slot_start_diff ASC
        ) AS rank_propagation
    FROM {{ source('clickhouse', 'beacon_api_eth_v1_events_attestation') }}
    WHERE
        slot_start_date_time BETWEEN '{{ run_times.start_time }}' - INTERVAL '{{ grace_period }}' AND '{{ run_times.end_time }}'
        AND attesting_validator_index IS NOT NULL
        AND meta_consensus_implementation = 'teku'
),

totals AS (
    SELECT
        slot,
        slot_start_date_time AS slot_started_at,
        meta_network_name AS network,
        attesting_validator_index AS validator_index,
        meta_client_geo_country AS nearest_client_country,
        meta_client_geo_continent_code
            AS nearest_client_continent_code,
        xxHash32( -- noqa: CP03
            CAST(slot AS String)
            || CAST(
                assumeNotNull(attesting_validator_index) AS String -- noqa: CP03
            )
            || meta_network_name
        ) AS unique_key,
        NOW() AS updated_at
    FROM ranked_data
    WHERE rank_propagation = 1
)

SELECT *
FROM totals
