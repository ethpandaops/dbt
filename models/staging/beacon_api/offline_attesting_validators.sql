{{ config(
    materialized="incremental",
    incremental_strategy="append",
    engine="ReplicatedReplacingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}/{uuid}', '{replica}', updated_at)",
    order_by="(unique_key)",
    unique_key="unique_key",
    sharding_key="unique_key",
    distributed=True,
) }}

-- this calculates what time window to populate.
-- depending on what data is available we need to make choices;
--  - does this model exist? if not, populate from the source
--  - if the this model empty? if so, populate from the source
--  - this model uses multiple sources so need 1 minute buffer from now()
--  - only front fill 4 hours at a time to not overload the db
--  - look back 30 minutes to replace data if needed
WITH min_max_slot_time AS (
    SELECT
        -- start_time
        CASE
            WHEN
                -- check if this model actually exists
                NOT EXISTS (
                    SELECT 1
                    FROM system.tables
                    WHERE database = 'dbt' AND name = '{{ this.name }}'
                )
                -- check if this model is empty
                OR (
                    SELECT MAX(slot_started_at)
                    FROM {{ this }}
                    WHERE network = 'mainnet'
                ) IS NULL
                -- fall back to the source beginning
                THEN MIN(slot_start_date_time)
            ELSE
                -- select the latest slot time minus 30 minutes
                (
                    SELECT MAX(slot_started_at) - INTERVAL 30 MINUTE
                    FROM {{ this }}
                    WHERE network = 'mainnet'
                )
        END AS start_time,
        -- end_time
        CASE
            WHEN
                -- check if this model actually exists
                NOT EXISTS (
                    SELECT 1
                    FROM system.tables
                    WHERE database = 'dbt' AND name = '{{ this.name }}'
                )
                -- check if this model is empty
                OR (
                    SELECT MAX(slot_started_at)
                    FROM {{ this }}
                    WHERE network = 'mainnet'
                ) IS NULL
                -- fall back to the source ending with 1 minute buffer
                THEN
                    CASE
                        WHEN
                            -- make sure never to go past NOW() - 1 minute
                            MAX(slot_start_date_time)
                            < NOW() - INTERVAL 1 MINUTE
                            THEN MAX(slot_start_date_time)
                        ELSE NOW() - INTERVAL 1 MINUTE
                    END
            WHEN
                -- check model latest slot time plus 4 hours is
                -- less than the source latest slot time
                (
                    SELECT MAX(slot_started_at) + INTERVAL 4 HOUR
                    FROM {{ this }}
                    WHERE network = 'mainnet'
                )
                <= MAX(slot_start_date_time)
                -- check if the model latest slot time plus 4 hours
                -- is less than NOW() - 1 minute
                AND (
                    SELECT MAX(slot_started_at) + INTERVAL 4 HOUR
                    FROM {{ this }}
                    WHERE network = 'mainnet'
                )
                < NOW() - INTERVAL 1 MINUTE
                -- this model is still front filling
                THEN
                    (
                        SELECT MAX(slot_started_at) + INTERVAL 4 HOUR
                        FROM {{ this }}
                        WHERE network = 'mainnet'
                    )
            -- check if the model latest slot time is less than NOW() - 1 minute
            WHEN MAX(slot_start_date_time) < NOW() - INTERVAL 1 MINUTE
                -- fill to the latest source slot time
                THEN MAX(slot_start_date_time)
            -- otherwise fill to NOW() - 1 minute
            ELSE NOW() - INTERVAL 1 MINUTE
        END AS end_time
    FROM
        {{ source('clickhouse', 'beacon_api_eth_v1_beacon_committee') }}
    WHERE
        slot_start_date_time >= NOW() - INTERVAL '2 DAY'
        AND meta_network_name = 'mainnet'
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
            SELECT start_time FROM min_max_slot_time
        ) AND (
            SELECT end_time FROM min_max_slot_time
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
            SELECT start_time FROM min_max_slot_time
        ) AND (
            SELECT end_time FROM min_max_slot_time
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
