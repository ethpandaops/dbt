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
        SELECT MAX(slot_started_at) - INTERVAL '15 MINUTE' AS start_time
        FROM {{ this }}
    {% else %}
        SELECT MIN(slot_start_date_time) AS start_time
        FROM {{ source('clickhouse', 'beacon_api_eth_v1_beacon_committee') }}
    {% endif %}
),

expected_validators AS (
    SELECT
        slot,
        slot_start_date_time,
        meta_network_name,
        arrayJoin(validators) AS validator_index -- noqa: CP03
    FROM
        {{ source('clickhouse', 'beacon_api_eth_v1_beacon_committee') }}
    WHERE
        slot_start_date_time BETWEEN (
            SELECT start_time FROM min_slot_time
        ) AND (
            SELECT start_time + INTERVAL '4 HOUR' FROM min_slot_time
        )
),

attested_validators AS (
    SELECT
        slot,
        slot_start_date_time,
        meta_network_name,
        attesting_validator_index AS validator_index
    FROM
        {{ source('clickhouse', 'beacon_api_eth_v1_events_attestation') }}
    WHERE
        slot_start_date_time BETWEEN (
            SELECT start_time FROM min_slot_time
        ) AND (
            SELECT start_time + INTERVAL '4 HOUR' FROM min_slot_time
        )
    GROUP BY
        slot, slot_start_date_time, meta_network_name, attesting_validator_index
),

missing_validators AS (
    SELECT
        ev.slot,
        ev.slot_start_date_time,
        ev.meta_network_name,
        ev.validator_index
    FROM
        expected_validators AS ev
    LEFT ANTI JOIN
        attested_validators AS att
        ON (
            ev.slot = att.slot
            AND ev.validator_index = att.validator_index
            AND ev.meta_network_name = att.meta_network_name
        )
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
        missing_validators AS av
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
