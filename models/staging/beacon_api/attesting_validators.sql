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
--  - this model uses multiple sources so need 12 seconds buffer from now()
--  - only front fill 4 hours at a time to not overload the db
--  - look back 30 minutes to replace data if needed
WITH min_max_slot_time AS (
    {% if is_incremental() %}
        SELECT
            -- start_time
            CASE
                WHEN
                    -- check if this model is empty
                    (
                        SELECT MAX(slot_started_at)
                        FROM {{ this }}
                    ) IS NULL
                    -- fall back to the source beginning
                    THEN MIN(slot_start_date_time)
                ELSE
                    -- select the latest slot time minus 30 minutes
                    (
                        SELECT MAX(slot_started_at) - INTERVAL 30 MINUTE
                        FROM {{ this }}
                    )
            END AS start_time,
            -- end_time
            CASE
                WHEN
                    -- check if this model is empty
                    (
                        SELECT MAX(slot_started_at)
                        FROM {{ this }}
                    ) IS NULL
                    -- fall back to the source ending with 1 minute buffer
                    THEN
                        CASE
                            WHEN
                                -- make sure never to go past NOW() - 12s
                                MAX(slot_start_date_time)
                                < NOW() - INTERVAL 12 SECOND
                                THEN MAX(slot_start_date_time)
                            ELSE NOW() - INTERVAL 12 SECOND
                        END
                WHEN
                    -- check model latest slot time plus 4 hours is
                    -- less than the source latest slot time
                    (
                        SELECT MAX(slot_started_at) + INTERVAL 4 HOUR
                        FROM {{ this }}
                    )
                    <= MAX(slot_start_date_time)
                    -- check if the model latest slot time plus 4 hours
                    -- is less than NOW() - 12s
                    AND (
                        SELECT MAX(slot_started_at) + INTERVAL 4 HOUR
                        FROM {{ this }}
                    )
                    < NOW() - INTERVAL 12 SECOND
                    -- this model is still front filling
                    THEN
                        (
                            SELECT MAX(slot_started_at) + INTERVAL 4 HOUR
                            FROM {{ this }}
                        )
                -- check if the model latest slot time is less than NOW() - 12s
                WHEN MAX(slot_start_date_time) < NOW() - INTERVAL 12 SECOND
                    -- fill to the latest source slot time
                    THEN MAX(slot_start_date_time)
                -- otherwise fill to NOW() - 12s
                ELSE NOW() - INTERVAL 12 SECOND
            END AS end_time
        FROM
            {{ source('clickhouse', 'beacon_api_eth_v1_events_attestation') }}
    {% else %}
        SELECT
            -- start_time
            MIN(slot_start_date_time) AS start_time,
            -- end_time
            CASE
                -- check if front filling
                WHEN MIN(slot_start_date_time) + INTERVAL 4 HOUR <= MAX(slot_start_date_time) AND MIN(slot_start_date_time) + INTERVAL 4 HOUR < NOW() - INTERVAL 12 SECOND
                THEN MIN(slot_start_date_time) + INTERVAL 4 HOUR
                -- check if source is less than NOW() - 12s
                WHEN MAX(slot_start_date_time) < NOW() - INTERVAL 12 SECOND
                THEN MAX(slot_start_date_time)
                -- otherwise fill to NOW() - 12s
                ELSE NOW() - INTERVAL 12 SECOND
            END AS end_time
        FROM
            {{ source('clickhouse', 'beacon_api_eth_v1_events_attestation') }}
    {% endif %}
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
