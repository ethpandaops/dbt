{% set interval = '1 MONTH' %}
{% set grace_period = '1 DAY' %}
{% set current_time = run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}

{{
    config(
        materialized='distributed_incremental',
        engine="ReplicatedReplacingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}/{uuid}', '{replica}', updated_at)",
        incremental_strategy='append',
        order_by="(slot_started_at, network)",
        unique_key="(slot_started_at, network)",
        sharding_key="cityHash64(slot_started_at, network)",
        post_hook="INSERT INTO {{ target.schema }}.model_metadata (model, updated_date_time, last_run_date_time) SELECT '{{ this }}', NOW(), CASE WHEN MAX(last_run_date_time) = '1970-01-01 00:00:00' THEN parseDateTime64BestEffortOrNull('" ~ current_time ~ "') ELSE LEAST(MAX(last_run_date_time) + INTERVAL " ~ interval ~ ", parseDateTime64BestEffortOrNull('" ~ current_time ~ "')) END as end_time FROM {{ target.schema }}.model_metadata FINAL WHERE model = '{{ this }}'"
    )
}}

{% set run_times = check_model_metadata_run_times(this, "'" ~ current_time ~ "'", interval) %}

WITH event_blocks AS (
    SELECT
        slot,
        MIN(slot_start_date_time) AS slot_started_at,
        epoch,
        CAST(block AS Nullable(FixedString(66))) AS beacon_block_root_hash,
        meta_network_name AS network,
        COUNT(*) AS total_seen_beacon_api_event_stream
    FROM
        {{ source('clickhouse', 'beacon_api_eth_v1_events_block') }} -- TODO FINAL
    WHERE
        slot_start_date_time BETWEEN '{{ run_times.start_time }}' - INTERVAL '{{ grace_period }}' AND '{{ run_times.end_time }}'
    GROUP BY slot, epoch, network, beacon_block_root_hash
),
gossip_blocks AS (
    SELECT
        slot,
        MIN(slot_start_date_time) AS slot_started_at,
        epoch,
        meta_network_name AS network,
        CAST(block AS Nullable(FixedString(66))) AS beacon_block_root_hash,
        COUNT(*) AS total_seen_gossipsub
    FROM
        {{ source('clickhouse', 'libp2p_gossipsub_beacon_block') }} FINAL
    WHERE
        slot_start_date_time BETWEEN '{{ run_times.start_time }}' - INTERVAL '{{ grace_period }}' AND '{{ run_times.end_time }}'
    GROUP BY slot, epoch, network, beacon_block_root_hash
),
canonical_blocks AS (
    SELECT
        slot,
        MIN(slot_start_date_time) AS slot_started_at,
        epoch,
        meta_network_name AS network,
        CAST(block_root AS Nullable(FixedString(66))) AS beacon_block_root_hash
    FROM
        {{ source('clickhouse', 'canonical_beacon_block') }} FINAL
    WHERE
        slot_start_date_time BETWEEN '{{ run_times.start_time }}' - INTERVAL '{{ grace_period }}' AND '{{ run_times.end_time }}'
    GROUP BY slot, epoch, network, block_root
),
combined_blocks AS (
    SELECT
        COALESCE(e.slot, g.slot, c.slot) AS slot,
        COALESCE(e.slot_started_at, g.slot_started_at, c.slot_started_at) AS slot_started_at,
        COALESCE(e.epoch, g.epoch, c.epoch) AS epoch,
        COALESCE(e.network, g.network, c.network) AS network,
        c.beacon_block_root_hash AS canonical_beacon_block_root_hash,
        arrayJoin(if(e.beacon_block_root_hash IS NOT NULL, [e.beacon_block_root_hash], [NULL])) AS non_canonical_beacon_api_event_stream_block_root_hash,
        arrayJoin(if(g.beacon_block_root_hash IS NOT NULL, [g.beacon_block_root_hash], [NULL])) AS non_canonical_gossipsub_block_root_hash,
        e.total_seen_beacon_api_event_stream AS total_seen_beacon_api_event_stream,
        g.total_seen_gossipsub AS total_seen_gossipsub
    FROM
        event_blocks e
    FULL OUTER JOIN
        gossip_blocks g
        ON e.slot = g.slot AND e.epoch = g.epoch AND e.network = g.network
    FULL OUTER JOIN
        canonical_blocks c
        ON COALESCE(e.slot, g.slot) = c.slot AND COALESCE(e.epoch, g.epoch) = c.epoch AND COALESCE(e.network, g.network) = c.network
),
max_canonical AS (
    SELECT
        slot,
        slot_started_at,
        epoch,
        network,
        MAX(canonical_beacon_block_root_hash) AS canonical_beacon_block_root_hash
    FROM
        combined_blocks
    GROUP BY slot, slot_started_at, epoch, network
)
SELECT
    NOW() AS updated_at,
    cb.slot,
    cb.slot_started_at,
    cb.epoch,
    cb.network,
    mc.canonical_beacon_block_root_hash AS canonical_beacon_block_root_hash,
    arrayFilter(x -> x IS NOT NULL AND (mc.canonical_beacon_block_root_hash IS NULL OR x != mc.canonical_beacon_block_root_hash), groupUniqArray(cb.non_canonical_beacon_api_event_stream_block_root_hash)) AS non_canonical_beacon_api_event_stream_block_root_hashes,
    arrayFilter(x -> x IS NOT NULL AND (mc.canonical_beacon_block_root_hash IS NULL OR x != mc.canonical_beacon_block_root_hash), groupUniqArray(cb.non_canonical_gossipsub_block_root_hash)) AS non_canonical_gossipsub_block_root_hashes,
    max(cb.total_seen_beacon_api_event_stream) AS total_seen_beacon_api_event_stream,
    max(cb.total_seen_gossipsub) AS total_seen_gossipsub
FROM
    combined_blocks cb
LEFT JOIN
    max_canonical mc
    ON cb.slot = mc.slot AND cb.slot_started_at = mc.slot_started_at AND cb.epoch = mc.epoch AND cb.network = mc.network
GROUP BY
    cb.slot, cb.slot_started_at, cb.epoch, cb.network, mc.canonical_beacon_block_root_hash
