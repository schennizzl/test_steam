select
    json_format(
        cast(
            map(
                array[
                    'appid',
                    'game_name',
                    'twitch_lookup_name',
                    'twitch_category_id',
                    'twitch_category_name',
                    'approx_total_viewers',
                    'live_channels',
                    'pages_fetched',
                    'is_partial',
                    'source_file',
                    'ingested_at',
                    'dt',
                    'hour'
                ],
                array[
                    cast(cast(appid as bigint) as json),
                    cast(cast(game_name as varchar) as json),
                    cast(cast(twitch_lookup_name as varchar) as json),
                    cast(cast(twitch_category_id as varchar) as json),
                    cast(cast(twitch_category_name as varchar) as json),
                    cast(cast(approx_total_viewers as integer) as json),
                    cast(cast(live_channels as integer) as json),
                    cast(cast(pages_fetched as integer) as json),
                    cast(cast(is_partial as boolean) as json),
                    cast(cast(source_file as varchar) as json),
                    cast(cast(ingested_at as varchar) as json),
                    cast(cast(dt as date) as json),
                    cast(cast(hour as integer) as json)
                ]
            ) as json
        )
    ) as payload,
    cast(replace(substr(ingested_at, 1, 19), 'T', ' ') as timestamp(3)) as upload_dt
from {{ source('landing', 'twitch_viewers_files') }}
where appid is not null
