{{ config(
    materialized="incremental",
    incremental_strategy="append",
    engine=" ReplicatedReplacingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}/{uuid}', '{replica}', updated_at)",
    order_by="(unique_key)",
    unique_key="unique_key",
    sharding_key="unique_key",
    distributed=True,
) }}

WITH window_boundaries AS (
    SELECT
        {% if is_incremental() %}
            (SELECT MAX(slot_started_at) - INTERVAL '1 MINUTE' FROM {{ this }})
        {% else %}
            (SELECT MIN(slot_start_date_time) FROM {{ source('clickhouse', 'beacon_api_eth_v1_validator_attestation_data') }})
        {% endif %}
            AS start_time,
        {% if is_incremental() %}
            (SELECT MAX(slot_started_at) + INTERVAL '3 DAY' FROM {{ this }})
        {% else %}
            (SELECT MIN(slot_start_date_time) + INTERVAL '3 DAY' FROM {{ source('clickhouse', 'beacon_api_eth_v1_validator_attestation_data') }})
        {% endif %}
            AS end_time
),

attestation_divergence AS (
    WITH aggregated AS (
        SELECT
            a.slot_start_date_time,
            a.slot,
            a.epoch,
            a.committee_index,
            a.meta_network_name,
            a.meta_consensus_implementation,
            cityHash64( -- noqa: CP03
                a.beacon_block_root,
                a.source_epoch,
                a.source_root,
                a.target_epoch,
                a.target_root
            ) AS hash,
            COUNT() AS cnt
        FROM
            {{
                source('clickhouse', 'beacon_api_eth_v1_validator_attestation_data')
            }} AS a INNER JOIN window_boundaries AS b ON 1 = 1
        WHERE
            a.slot_start_date_time >= b.start_time
            AND a.slot_start_date_time <= b.end_time
        GROUP BY
            a.slot_start_date_time,
            a.slot,
            a.epoch,
            a.committee_index,
            a.meta_network_name,
            hash,
            a.meta_consensus_implementation
    ),

    maxhash AS (
        SELECT
            slot_start_date_time,
            slot,
            epoch,
            committee_index,
            meta_network_name,
            meta_consensus_implementation,
            argMax(hash, cnt) AS largest_hash, -- noqa: CP03
            MAX(cnt) AS maxcnt
        FROM
            aggregated
        GROUP BY
            slot_start_date_time,
            slot,
            epoch,
            committee_index,
            meta_network_name,
            meta_consensus_implementation
    ),

    totalcounts AS (
        SELECT
            slot_start_date_time,
            slot,
            epoch,
            committee_index,
            meta_network_name,
            meta_consensus_implementation,
            SUM(cnt) AS totalcnt
        FROM
            aggregated
        GROUP BY
            slot_start_date_time,
            slot,
            epoch,
            committee_index,
            meta_network_name,
            meta_consensus_implementation
    )

    SELECT
        m.slot_start_date_time AS slot_started_at, -- noqa: CP03
        m.slot AS slot,
        m.epoch AS epoch,
        m.committee_index AS committee_index,
        m.meta_network_name AS network,
        cityHash64( -- noqa: CP03
            m.slot_start_date_time,
            m.slot,
            m.committee_index,
            m.meta_network_name
        ) AS unique_key,
        NOW() AS updated_at,

        CASE
            WHEN
                MAX(
                    IF(
                        m.meta_consensus_implementation = 'prysm'
                        AND m.maxcnt = t.totalcnt,
                        1,
                        0
                    )
                )
                = 1
                AND MAX(
                    IF(
                        m.meta_consensus_implementation = 'teku'
                        AND m.maxcnt = t.totalcnt,
                        1,
                        0
                    )
                )
                = 1
                AND MAX(
                    IF(
                        m.meta_consensus_implementation = 'lodestar'
                        AND m.maxcnt = t.totalcnt,
                        1,
                        0
                    )
                )
                = 1
                AND MAX(
                    IF(
                        m.meta_consensus_implementation = 'lighthouse'
                        AND m.maxcnt = t.totalcnt,
                        1,
                        0
                    )
                )
                = 1
                AND MAX(
                    IF(
                        m.meta_consensus_implementation = 'nimbus'
                        AND m.maxcnt = t.totalcnt,
                        1,
                        0
                    )
                )
                = 1
                THEN 1
            ELSE 0
        END AS all_equal,

        MAX(
            IF(
                m.meta_consensus_implementation = 'prysm'
                AND m.maxcnt = t.totalcnt,
                1,
                0
            )
        ) AS prysm_all_equal,
        MAX(
            IF(
                m.meta_consensus_implementation = 'teku'
                AND m.maxcnt = t.totalcnt,
                1,
                0
            )
        ) AS teku_all_equal,
        MAX(
            IF(
                m.meta_consensus_implementation = 'lodestar'
                AND m.maxcnt = t.totalcnt,
                1,
                0
            )
        ) AS lodestar_all_equal,
        MAX(
            IF(
                m.meta_consensus_implementation = 'lighthouse'
                AND m.maxcnt = t.totalcnt,
                1,
                0
            )
        ) AS lighthouse_all_equal,
        MAX(
            IF(
                m.meta_consensus_implementation = 'nimbus'
                AND m.maxcnt = t.totalcnt,
                1,
                0
            )
        ) AS nimbus_all_equal

    FROM
        aggregated AS a
    INNER JOIN maxhash AS m
        ON
            a.slot_start_date_time = m.slot_start_date_time
            AND a.slot = m.slot
            AND a.epoch = m.epoch
            AND a.committee_index = m.committee_index
            AND a.meta_network_name = m.meta_network_name
    INNER JOIN totalcounts AS t
        ON
            a.slot_start_date_time = m.slot_start_date_time
            AND m.slot = t.slot
            AND a.epoch = m.epoch
            AND m.committee_index = t.committee_index
            AND a.meta_network_name = m.meta_network_name
            AND m.meta_consensus_implementation
            = t.meta_consensus_implementation
    GROUP BY
        m.slot_start_date_time,
        m.slot,
        m.epoch,
        m.committee_index,
        m.meta_network_name
    ORDER BY
        slot ASC,
        committee_index ASC
)

SELECT *
FROM attestation_divergence
