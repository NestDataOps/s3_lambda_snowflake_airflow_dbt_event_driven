with source as (
    select * from {{ source('raw', 'raw_events') }}
),

flattened as (
    select
        event_id,
        event_type,
        source_file_name,
        ingested_at,
        event_payload:user_id::string        as user_id,
        event_payload:amount::float          as amount,
        event_payload:currency::string       as currency,
        event_payload:created_at::timestamp  as event_created_at
    from source
),

-- Snowflake's COPY INTO dedup is per-FILE (path + checksum), not per-event
-- -- if the same event content ever gets staged again under a different
-- filename (a re-upload, a retry, manual re-testing), it loads again as a
-- genuinely "new" file and duplicates rows in raw_events. Guard against
-- that here rather than trying to make the load step itself perfectly
-- duplicate-proof. Keeps the most recently ingested copy of each event_id.
deduped as (
    select *
    from flattened
    qualify row_number() over (
        partition by event_id
        order by ingested_at desc
    ) = 1
)

select * from deduped
