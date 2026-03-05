select
    cast(json_extract_scalar(payload, '$.appid') as bigint) as appid,
    json_extract_scalar(payload, '$.game_name') as game_name,
    json_extract_scalar(payload, '$.twitch_lookup_name') as twitch_lookup_name,
    json_extract_scalar(payload, '$.twitch_category_id') as twitch_category_id,
    json_extract_scalar(payload, '$.twitch_category_name') as twitch_category_name,
    cast(json_extract_scalar(payload, '$.approx_total_viewers') as integer) as approx_total_viewers,
    cast(json_extract_scalar(payload, '$.live_channels') as integer) as live_channels,
    cast(json_extract_scalar(payload, '$.pages_fetched') as integer) as pages_fetched,
    cast(json_extract_scalar(payload, '$.is_partial') as boolean) as is_partial,
    cast(json_extract_scalar(payload, '$.dt') as date) as event_date,
    cast(json_extract_scalar(payload, '$.hour') as integer) as event_hour,
    upload_dt as ingested_at,
    json_extract_scalar(payload, '$.source_file') as source_file
from {{ ref('raw_steam_twitch_viewers') }}
