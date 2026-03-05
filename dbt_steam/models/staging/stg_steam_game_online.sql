select
    cast(json_extract_scalar(payload, '$.appid') as bigint) as appid,
    json_extract_scalar(payload, '$.game_name') as game_name,
    cast(json_extract_scalar(payload, '$.current_players') as integer) as current_players,
    cast(json_extract_scalar(payload, '$.dt') as date) as event_date,
    cast(json_extract_scalar(payload, '$.hour') as integer) as event_hour,
    upload_dt as ingested_at,
    json_extract_scalar(payload, '$.source_file') as source_file
from {{ ref('raw_steam_game_online') }}
