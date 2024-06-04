{% set interval = '1 MONTH' %}
{% set grace_period = '1 DAY' %}
{% set current_time = run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}

{{
    config(
        materialized='distributed_incremental',
        engine="ReplicatedReplacingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}/{uuid}', '{replica}', updated_at)",
        incremental_strategy='append',
        order_by="(beacon_block_root_hash, network)",
        unique_key="(beacon_block_root_hash, network)",
        sharding_key="cityHash64(beacon_block_root_hash, network)",
        post_hook="INSERT INTO {{ target.schema }}.model_metadata (model, updated_date_time, last_run_date_time) SELECT '{{ this }}', NOW(), CASE WHEN MAX(last_run_date_time) = '1970-01-01 00:00:00' THEN parseDateTime64BestEffortOrNull('" ~ current_time ~ "') ELSE LEAST(MAX(last_run_date_time) + INTERVAL " ~ interval ~ ", parseDateTime64BestEffortOrNull('" ~ current_time ~ "')) END as end_time FROM {{ target.schema }}.model_metadata FINAL WHERE model = '{{ this }}'"
    )
}}

{% set run_times = check_model_metadata_run_times(this, "'" ~ current_time ~ "'", interval) %}

WITH event_blobs AS (
    SELECT
        CAST(slot AS Nullable(Int32)) AS slot,
        MIN(CAST(slot_start_date_time AS Nullable(DateTime))) AS slot_started_at,
        CAST(epoch AS Nullable(Int32)) AS epoch,
        CAST(meta_network_name AS Nullable(String)) AS network,
        CAST(block_root AS Nullable(FixedString(66))) AS beacon_block_root_hash,
        CAST(max(blob_index) + 1 AS Nullable(UInt64)) AS blob_count
    FROM
        {{ source('clickhouse', 'beacon_api_eth_v1_events_blob_sidecar') }} -- TODO FINAL
    WHERE
        slot_start_date_time BETWEEN '{{ run_times.start_time }}' - INTERVAL '{{ grace_period }}' AND '{{ run_times.end_time }}'
    GROUP BY slot, epoch, block_root, network
),
canonical_blobs AS (
    SELECT
        CAST(slot AS Nullable(Int32)) AS slot,
        MIN(CAST(slot_start_date_time AS Nullable(DateTime))) AS slot_started_at,
        CAST(epoch AS Nullable(Int32)) AS epoch,
        CAST(meta_network_name AS Nullable(String)) AS network,
        CAST(block_root AS Nullable(FixedString(66))) AS beacon_block_root_hash,
        CAST(max(blob_index) + 1 AS Nullable(UInt64)) AS blob_count
    FROM
        {{ source('clickhouse', 'canonical_beacon_blob_sidecar') }} FINAL
    WHERE
        slot_start_date_time BETWEEN '{{ run_times.start_time }}' - INTERVAL '{{ grace_period }}' AND '{{ run_times.end_time }}'
    GROUP BY slot, epoch, block_root, network
),
-- need to map state root to block root for libp2p_gossipsub_blob_sidecar
beacon_api_state_root_map AS (
    SELECT
        CAST(slot AS Nullable(Int32)) AS slot,
        CAST(block_root AS Nullable(FixedString(66))) AS beacon_block_root_hash,
        CAST(meta_network_name AS Nullable(String)) AS network,
        CAST(state_root AS Nullable(FixedString(66))) AS beacon_state_root_hash
    FROM
        {{ source('clickhouse', 'beacon_api_eth_v2_beacon_block') }} -- TODO FINAL
    WHERE
        slot_start_date_time BETWEEN '{{ run_times.start_time }}' - INTERVAL '{{ grace_period }}' AND '{{ run_times.end_time }}'
    GROUP BY slot, block_root, network, state_root
),
canonical_state_root_map AS (
    SELECT
        CAST(slot AS Nullable(Int32)) AS slot,
        CAST(block_root AS Nullable(FixedString(66))) AS beacon_block_root_hash,
        CAST(meta_network_name AS Nullable(String)) AS network,
        CAST(state_root AS Nullable(FixedString(66))) AS beacon_state_root_hash
    FROM
        {{ source('clickhouse', 'canonical_beacon_block') }} FINAL
    WHERE
        slot_start_date_time BETWEEN '{{ run_times.start_time }}' - INTERVAL '{{ grace_period }}' AND '{{ run_times.end_time }}'
    GROUP BY slot, block_root, network, state_root
),
state_root_map AS (
    SELECT
        *
    FROM beacon_api_state_root_map
    UNION ALL
    SELECT
        *
    FROM canonical_state_root_map
),
gossipsub_blobs_state_root AS (
    SELECT
        CAST(slot AS Nullable(Int32)) AS slot,
        MIN(CAST(slot_start_date_time AS Nullable(DateTime))) AS slot_started_at,
        CAST(epoch AS Nullable(Int32)) AS epoch,
        CAST(meta_network_name AS Nullable(String)) AS network,
        CAST(state_root AS Nullable(FixedString(66))) AS beacon_state_root_hash,
        CAST(max(blob_index) + 1 AS Nullable(UInt64)) AS blob_count
    FROM
        {{ source('clickhouse', 'libp2p_gossipsub_blob_sidecar') }} FINAL
    WHERE
        slot_start_date_time BETWEEN '{{ run_times.start_time }}' - INTERVAL '{{ grace_period }}' AND '{{ run_times.end_time }}'
    GROUP BY slot, epoch, state_root, network
),
-- map state root to block root
gossipsub_blobs AS (
    SELECT
        g.slot,
        g.slot_started_at,
        g.epoch,
        g.network,
        CAST(s.beacon_block_root_hash AS Nullable(FixedString(66))) AS beacon_block_root_hash,
        g.blob_count
    FROM gossipsub_blobs_state_root AS g
    LEFT JOIN state_root_map AS s
        ON g.slot = s.slot AND g.beacon_state_root_hash = s.beacon_state_root_hash AND g.network = s.network
),
combined_blobs AS (
    SELECT
        CAST(COALESCE(c.slot, e.slot, g.slot) AS Int32) AS slot,
        CAST(COALESCE(c.slot_started_at, e.slot_started_at, g.slot_started_at) AS DateTime) AS slot_started_at,
        CAST(COALESCE(c.epoch, e.epoch, g.epoch) AS Int32) AS epoch,
        CAST(COALESCE(c.network, e.network, g.network) AS String) AS network,
        CAST(COALESCE(c.beacon_block_root_hash, e.beacon_block_root_hash, g.beacon_block_root_hash) AS FixedString(66)) AS beacon_block_root_hash,
        CAST(COALESCE(c.blob_count, e.blob_count, g.blob_count) AS UInt64) AS blob_count
    FROM
        event_blobs e
    FULL OUTER JOIN
        gossipsub_blobs g
        ON e.slot = g.slot AND e.epoch = g.epoch AND e.network = g.network
    FULL OUTER JOIN
        canonical_blobs c
        ON COALESCE(e.slot, g.slot) = c.slot AND COALESCE(e.epoch, g.epoch) = c.epoch AND COALESCE(e.network, g.network) = c.network
)
SELECT
    NOW() AS updated_at,
    slot,
    slot_started_at,
    epoch,
    network,
    beacon_block_root_hash,
    MAX(blob_count) AS blob_count
FROM
    combined_blobs
GROUP BY slot, slot_started_at, epoch, network, beacon_block_root_hash
