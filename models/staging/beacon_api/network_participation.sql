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

WITH attesting AS (
    SELECT
        slot,
        slot_start_date_time,
        meta_network_name,
        COUNT(DISTINCT attesting_validator_index) AS attesting_validators
    FROM
        {{ source('clickhouse', 'beacon_api_eth_v1_events_attestation') }}
    WHERE
        slot_start_date_time BETWEEN '{{ run_times.start_time }}' - INTERVAL '{{ grace_period }}' AND '{{ run_times.end_time }}'
    GROUP BY slot, slot_start_date_time, meta_network_name
),

total AS (
    SELECT
        slot,
        slot_start_date_time,
        meta_network_name,
        SUM(total_validators_committee) AS total_validators
    FROM (
        SELECT
            slot,
            slot_start_date_time,
            meta_network_name,
            committee_index,
            MAX(LENGTH(validators)) AS total_validators_committee
        FROM
            {{ source('clickhouse', 'beacon_api_eth_v1_beacon_committee') }}
        WHERE
            slot_start_date_time BETWEEN '{{ run_times.start_time }}' - INTERVAL '{{ grace_period }}' AND '{{ run_times.end_time }}'
        GROUP BY slot, slot_start_date_time, meta_network_name, committee_index
    )
    GROUP BY slot, slot_start_date_time, meta_network_name
),

participation AS (
    SELECT
        attesting.slot AS slot,
        attesting.slot_start_date_time AS slot_started_at,
        attesting.meta_network_name AS network,
        attesting.attesting_validators,
        total.total_validators,
        attesting.attesting_validators * 1.0
        / total.total_validators AS participation_rate,
        xxHash32( -- noqa: CP03
            CAST(attesting.slot AS String) || attesting.meta_network_name
        ) AS unique_key,
        NOW() AS updated_at
    FROM attesting
    INNER JOIN
        total
        ON
            attesting.slot_start_date_time = total.slot_start_date_time
            AND attesting.meta_network_name = total.meta_network_name
            AND attesting.slot = total.slot
)

SELECT *
FROM participation
