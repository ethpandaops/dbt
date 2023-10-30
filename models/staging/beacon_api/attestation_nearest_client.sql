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
--  - look back 1 minutes to replace data if needed
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
                    -- select the latest slot time minus 1 minutes
                    (
                        SELECT MAX(slot_started_at) - INTERVAL 1 MINUTE
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
                        SELECT MAX(slot_started_at) + INTERVAL 1 HOUR
                        FROM {{ this }}
                    )
                    <= MAX(slot_start_date_time)
                    -- check if the model latest slot time plus 4 hours
                    -- is less than NOW() - 12s
                    AND (
                        SELECT MAX(slot_started_at) + INTERVAL 1 HOUR
                        FROM {{ this }}
                    )
                    < NOW() - INTERVAL 12 SECOND
                    -- this model is still front filling
                    THEN
                        (
                            SELECT MAX(slot_started_at) + INTERVAL 1 HOUR
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
                WHEN MIN(slot_start_date_time) + INTERVAL 1 HOUR <= MAX(slot_start_date_time) AND MIN(slot_start_date_time) + INTERVAL 1 HOUR < NOW() - INTERVAL 12 SECOND
                THEN MIN(slot_start_date_time) + INTERVAL 1 HOUR
                -- check if source is less than NOW() - 12s
                WHEN MAX(slot_start_date_time) < NOW() - INTERVAL 12 SECOND
                THEN MAX(slot_start_date_time)
                -- otherwise fill to NOW() - 12s
                ELSE NOW() - INTERVAL 12 SECOND
            END AS end_time
        FROM
            {{ source('clickhouse', 'beacon_api_eth_v1_events_attestation') }}
        WHERE attesting_validator_index IS NOT NULL
    {% endif %}
),

ranked_data AS (
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
        slot_start_date_time BETWEEN (
            SELECT start_time FROM min_max_slot_time
        ) AND (
            SELECT end_time FROM min_max_slot_time
        )
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
