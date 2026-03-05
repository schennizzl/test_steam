select
    cast(json_extract_scalar(payload, '$.appid') as bigint) as appid,
    json_extract_scalar(payload, '$.name') as app_name,
    json_extract_scalar(payload, '$.type') as app_type,
    cast(json_extract_scalar(payload, '$.dt') as date) as snapshot_date,
    upload_dt as ingested_at,
    json_extract_scalar(payload, '$.source_file') as source_file
from {{ ref('raw_steam_store_daily') }}
