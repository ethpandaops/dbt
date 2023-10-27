{{ config(
    materialized="incremental",
    incremental_strategy="append",
    engine="ReplicatedReplacingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}/{uuid}', '{replica}', updated_at)",
    order_by="(unique_key)",
    unique_key="unique_key",
    sharding_key="unique_key",
    distributed=True,
) }}

WITH min_max_slot_time AS (
    SELECT
        CASE
            WHEN NOT EXISTS (SELECT 1 FROM system.tables WHERE database = 'dbt' AND name = '{{ this.name }}')
            OR (SELECT MAX(slot_started_at) FROM {{ this }} WHERE network = 'mainnet') IS NULL
            THEN MIN(slot_start_date_time)
            ELSE (SELECT MAX(slot_started_at) - INTERVAL 30 MINUTE FROM {{ this }} WHERE network = 'mainnet')
        END AS start_time,
        CASE
            WHEN NOT EXISTS (SELECT 1 FROM system.tables WHERE database = 'dbt' AND name = '{{ this.name }}')
            OR (SELECT MAX(slot_started_at) FROM {{ this }} WHERE network = 'mainnet') IS NULL
            THEN
                CASE
                    WHEN MAX(slot_start_date_time) < NOW() - INTERVAL 1 MINUTE
                    THEN MAX(slot_start_date_time)
                    ELSE NOW() - INTERVAL 1 MINUTE
                END
            WHEN (SELECT MAX(slot_started_at) + INTERVAL 4 HOUR FROM {{ this }} WHERE network = 'mainnet') <= MAX(slot_start_date_time) AND (SELECT MAX(slot_started_at) + INTERVAL 4 HOUR FROM {{ this }} WHERE network = 'mainnet') < NOW() - INTERVAL 1 MINUTE
            THEN (SELECT MAX(slot_started_at) + INTERVAL 4 HOUR FROM {{ this }} WHERE network = 'mainnet')
            WHEN MAX(slot_start_date_time) < NOW() - INTERVAL 1 MINUTE
            THEN MAX(slot_start_date_time)
            ELSE NOW() - INTERVAL 1 MINUTE
        END AS end_time
    FROM
        {{ source('clickhouse', 'beacon_api_eth_v1_events_attestation') }}
    WHERE
        meta_network_name = 'mainnet'
),

attesting_validators AS (
    SELECT
        slot,
        slot_start_date_time,
        meta_network_name,
        attesting_validator_index AS validator_index
    FROM
        {{ source('clickhouse', 'beacon_api_eth_v1_events_attestation') }}
    WHERE
        slot_start_date_time BETWEEN (
            SELECT start_time FROM min_max_slot_time
        ) AND (
            SELECT end_time FROM min_max_slot_time
        )
    GROUP BY
        slot, slot_start_date_time, meta_network_name, attesting_validator_index
),

nearest_blockprint AS (
    SELECT
        av.slot,
        av.slot_start_date_time,
        av.meta_network_name,
        av.validator_index,
        argMin(b.best_guess_single, abs(b.slot - av.slot)) -- noqa: CP03
            AS best_guess
    FROM
        attesting_validators AS av
    LEFT JOIN
        {{
            source('clickhouse', 'beacon_block_classification')
        }} AS b FINAL ON (
        b.proposer_index = av.validator_index
        AND b.meta_network_name = av.meta_network_name
    )
    GROUP BY
        av.slot,
        av.slot_start_date_time,
        av.meta_network_name,
        av.validator_index
),

totals AS (
    SELECT
        nb.slot,
        nb.slot_start_date_time AS slot_started_at,
        nb.meta_network_name AS network,
        COALESCE(
            IF(nb.best_guess = '', 'unknown', nb.best_guess), 'unknown'
        ) AS guess,
        COUNT() AS occurrences,
        xxHash32( -- noqa: CP03
            CAST(nb.slot AS String) || nb.meta_network_name || guess
        ) AS unique_key,
        NOW() AS updated_at
    FROM
        nearest_blockprint AS nb
    GROUP BY
        nb.slot, nb.slot_start_date_time, nb.meta_network_name, nb.best_guess
)

SELECT *
FROM totals
