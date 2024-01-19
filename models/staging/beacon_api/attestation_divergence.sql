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
        post_hook="INSERT INTO {{ target.schema }}.model_metadata (unique_key, updated_date_time, model, last_run_date_time) SELECT cityHash64('{{ this }}'), NOW(), '{{ this }}', CASE WHEN MAX(last_run_date_time) = '1970-01-01 00:00:00' THEN parseDateTime64BestEffortOrNull('" ~ current_time ~ "') ELSE LEAST(MAX(last_run_date_time) + INTERVAL " ~ interval ~ ", parseDateTime64BestEffortOrNull('" ~ current_time ~ "')) END as end_time FROM {{ target.schema }}.model_metadata FINAL WHERE model = '{{ this }}'"
    )
}}

{% set run_times = check_model_metadata_run_times(this, "'" ~ current_time ~ "'", interval) %}

WITH attestation_divergence AS (
    WITH aggregated AS (
        SELECT
            slot_start_date_time,
            slot,
            epoch,
            committee_index,
            meta_network_name,
            meta_consensus_implementation,
            cityHash64( -- noqa: CP03
                beacon_block_root,
                source_epoch,
                source_root,
                target_epoch,
                target_root
            ) AS hash,
            COUNT() AS cnt
        FROM
            {{
                source('clickhouse', 'beacon_api_eth_v1_validator_attestation_data')
            }}
        WHERE
            slot_start_date_time BETWEEN '{{ run_times.start_time }}' - INTERVAL '{{ grace_period }}' AND '{{ run_times.end_time }}'
        GROUP BY
            slot_start_date_time,
            slot,
            epoch,
            committee_index,
            meta_network_name,
            hash,
            meta_consensus_implementation
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
