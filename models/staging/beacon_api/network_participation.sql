{{ config(
    materialized="incremental",
    incremental_strategy="append",
    engine="ReplicatedReplacingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}/{uuid}', '{replica}', updated_at)",
    order_by="(unique_key)",
    unique_key="unique_key",
    sharding_key="unique_key",
    distributed=True,
) }}

WITH min_slot_time AS (
    {% if is_incremental() %}
        SELECT MAX(slot_started_at) - INTERVAL '1 MINUTE' AS start_time
        FROM {{ this }}
    {% else %}
        SELECT MIN(slot_start_date_time) AS start_time
        FROM {{ source('clickhouse', 'beacon_api_eth_v1_beacon_committee') }}
    {% endif %}
),

attesting AS (
    SELECT
        slot,
        slot_start_date_time,
        meta_network_name,
        COUNT(DISTINCT attesting_validator_index) AS attesting_validators
    FROM
        {{ source('clickhouse', 'beacon_api_eth_v1_events_attestation') }}
    WHERE
        slot_start_date_time BETWEEN (
            SELECT start_time FROM min_slot_time
        ) AND (
            SELECT start_time + INTERVAL '1 DAY' FROM min_slot_time
        )
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
            slot_start_date_time BETWEEN (
                SELECT start_time FROM min_slot_time
            ) AND (
                SELECT start_time + INTERVAL '1 DAY' FROM min_slot_time
            )
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
