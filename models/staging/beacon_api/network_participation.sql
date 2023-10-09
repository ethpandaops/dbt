{{ config(
    materialized="incremental",
    incremental_strategy="append",
    engine="ReplicatedReplacingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}/{uuid}', '{replica}', updated_at)",
    order_by="(unique_key)",
    unique_key="unique_key",
    sharding_key="unique_key",
    distributed=True,
) }}

WITH Attesting AS (
    SELECT
        slot,
        slot_start_date_time,
        meta_network_name,
        COUNT(DISTINCT attesting_validator_index) AS attesting_validators
    FROM
        { source('clickhouse', 'beacon_api_eth_v1_events_attestation') }}
    {% if is_incremental() %}
        WHERE slot_start_date_time >= (
            SELECT MAX(slot_started_at) - INTERVAL '15 MINUTE'
            FROM {{ this }}
        )
    {% endif %}
    GROUP BY slot, slot_start_date_time, meta_network_name
),
Total AS (
    SELECT 
        slot,
        slot_start_date_time,
        meta_network_name,
        sum(length(validators)) AS total_validators
    FROM (
        SELECT
            slot,
            slot_start_date_time,
            meta_network_name,
            committee_index,
            validators
        FROM
            { source('clickhouse', 'beacon_api_eth_v1_beacon_committee') }}
        {% if is_incremental() %}
            WHERE slot_start_date_time >= (
                SELECT MAX(slot_started_at) - INTERVAL '15 MINUTE'
                FROM {{ this }}
            )
        {% endif %}
        LIMIT 1 BY slot_start_date_time, committee_index
    )
    GROUP BY slot, slot_start_date_time, meta_network_name
),
Participation AS (
    SELECT

        Attesting.slot as slot,
        Attesting.slot_start_date_time as slot_started_at,
        Attesting.meta_network_name as network,
        attesting_validators,
        total_validators,
        attesting_validators * 1.0 / total_validators AS participation_rate,
        xxHash32(CAST(slot AS String) || network) AS unique_key,
        NOW() AS updated_at
    FROM Attesting
    JOIN Total ON Attesting.slot_start_date_time = Total.slot_start_date_time AND Attesting.meta_network_name = Total.meta_network_name AND Attesting.slot = Total.slot
)

SELECT * 
FROM Participation

{% if is_incremental() %}
    WHERE slot_started_at >= (
        SELECT MAX(slot_started_at) - INTERVAL '15 MINUTE'
        FROM {{ this }}
    )
{% endif %}
