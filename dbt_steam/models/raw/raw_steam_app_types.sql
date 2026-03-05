select
    json_format(
        cast(
            map(
                array['appid', 'name', 'type', 'source_file', 'ingested_at', 'dt', 'hour'],
                array[
                    cast(cast(appid as bigint) as json),
                    cast(cast(name as varchar) as json),
                    cast(cast(type as varchar) as json),
                    cast(cast(source_file as varchar) as json),
                    cast(cast(ingested_at as varchar) as json),
                    cast(cast(dt as date) as json),
                    cast(cast(hour as integer) as json)
                ]
            ) as json
        )
    ) as payload,
    cast(replace(substr(ingested_at, 1, 19), 'T', ' ') as timestamp(3)) as upload_dt
from {{ source('landing', 'appdetails_types_files') }}
where appid is not null
