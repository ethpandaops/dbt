{{ config(
    materialized="incremental",
    incremental_strategy="append",
    engine=" ReplicatedReplacingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}/{uuid}', '{replica}', updated_at)",
    order_by="(unique_key)",
    unique_key="unique_key",
    sharding_key="unique_key",
    distributed=True,
) }}

WITH attestation_divergence AS (
    WITH Aggregated AS (
        SELECT
            slot_start_date_time,
            slot,
            epoch,
            committee_index,
            meta_network_name,
            cityHash64(beacon_block_root, source_epoch, source_root, target_epoch, target_root) AS hash,
            meta_consensus_implementation,
            count() AS cnt
        FROM
            {{ source('clickhouse', 'beacon_api_eth_v1_validator_attestation_data') }}
        {% if is_incremental() %}
            WHERE slot_start_date_time >= (
                SELECT MAX(slot_started_at) - INTERVAL '1 MINUTE'
                FROM {{ this }}
            )
        {% endif %}
        GROUP BY
            slot_start_date_time,
            slot,
            epoch,
            committee_index,
            meta_network_name,
            hash,
            meta_consensus_implementation
    ),

    MaxHash AS (
        SELECT
            slot_start_date_time,
            slot,
            epoch,
            committee_index,
            meta_network_name,
            meta_consensus_implementation,
            argMax(hash, cnt) AS largest_hash,
            max(cnt) AS maxCnt
        FROM
            Aggregated
        GROUP BY
            slot_start_date_time,
            slot,
            epoch,
            committee_index,
            meta_network_name,
            meta_consensus_implementation
    ),

    TotalCounts AS (
        SELECT
            slot_start_date_time,
            slot,
            epoch,
            committee_index,
            meta_network_name,
            meta_consensus_implementation,
            sum(cnt) AS totalCnt
        FROM
            Aggregated
        GROUP BY
            slot_start_date_time,
            slot,
            epoch,
            committee_index,
            meta_network_name,
            meta_consensus_implementation
    )

    SELECT
        cityHash64(M.slot_start_date_time, M.slot, M.committee_index, M.meta_network_name) AS unique_key, -- noqa: CP03
        NOW() AS updated_at,
        M.slot_start_date_time as slot_started_at,
        M.slot as slot,
        M.epoch as epoch,
        M.committee_index as committee_index,
        M.meta_network_name as network,
        
        CASE 
            WHEN 
                MAX(if(M.meta_consensus_implementation = 'prysm' AND M.maxCnt = T.totalCnt, 1, 0)) = 1
                AND MAX(if(M.meta_consensus_implementation = 'teku' AND M.maxCnt = T.totalCnt, 1, 0)) = 1
                AND MAX(if(M.meta_consensus_implementation = 'lodestar' AND M.maxCnt = T.totalCnt, 1, 0)) = 1
                AND MAX(if(M.meta_consensus_implementation = 'lighthouse' AND M.maxCnt = T.totalCnt, 1, 0)) = 1
                AND MAX(if(M.meta_consensus_implementation = 'nimbus' AND M.maxCnt = T.totalCnt, 1, 0)) = 1
            THEN 1
            ELSE 0
        END AS all_equal,

        MAX(if(M.meta_consensus_implementation = 'prysm' AND M.maxCnt = T.totalCnt, 1, 0)) AS prysm_all_equal,
        MAX(if(M.meta_consensus_implementation = 'teku' AND M.maxCnt = T.totalCnt, 1, 0)) AS teku_all_equal,
        MAX(if(M.meta_consensus_implementation = 'lodestar' AND M.maxCnt = T.totalCnt, 1, 0)) AS lodestar_all_equal,
        MAX(if(M.meta_consensus_implementation = 'lighthouse' AND M.maxCnt = T.totalCnt, 1, 0)) AS lighthouse_all_equal,
        MAX(if(M.meta_consensus_implementation = 'nimbus' AND M.maxCnt = T.totalCnt, 1, 0)) AS nimbus_all_equal

    FROM
        Aggregated A
    JOIN MaxHash M
        ON A.slot_start_date_time = M.slot_start_date_time AND A.slot = M.slot AND A.epoch = M.epoch AND A.committee_index = M.committee_index AND A.meta_network_name = M.meta_network_name
    JOIN TotalCounts T
        ON A.slot_start_date_time = M.slot_start_date_time AND M.slot = T.slot AND A.epoch = M.epoch AND M.committee_index = T.committee_index AND A.meta_network_name = M.meta_network_name AND M.meta_consensus_implementation = T.meta_consensus_implementation
    GROUP BY
        M.slot_start_date_time,
        M.slot,
        M.epoch,
        M.committee_index,
        M.meta_network_name
    ORDER BY
        slot ASC,
        committee_index ASC
)

SELECT *
FROM attestation_divergence

{% if is_incremental() %}

    WHERE slot_started_at >= (
        SELECT MAX(slot_started_at) - INTERVAL '1 MINUTE'
        FROM {{ this }}
    )

{% endif %}
