{% set snapshot_date = var('snapshot_date', none) %}

with source_rows as (
    select
        json_format(
            cast(
                map(
                    array['appid', 'name', 'type', 'source_file', 'ingested_at', 'dt'],
                    array[
                        cast(cast(appid as bigint) as json),
                        cast(cast(name as varchar) as json),
                        cast(cast(type as varchar) as json),
                        cast(cast(source_file as varchar) as json),
                        cast(cast(ingested_at as varchar) as json),
                        cast(cast(dt as date) as json)
                    ]
                ) as json
            )
        ) as payload,
        cast(source_file as varchar) as source_file,
        cast(replace(substr(ingested_at, 1, 19), 'T', ' ') as timestamp(3)) as upload_dt,
        cast(dt as date) as snapshot_date
    from {{ source('landing', 'store_daily_files') }}
    where appid is not null
),
target_snapshot as (
    select
        {% if snapshot_date is not none %}
        max(cast('{{ snapshot_date }}' as date)) as snapshot_date
        {% else %}
        max(snapshot_date) as snapshot_date
        {% endif %}
    from source_rows
),
latest_snapshot_file as (
    select
        source_file,
        upload_dt,
        row_number() over (
            order by upload_dt desc, source_file desc
        ) as rn
    from source_rows
    where snapshot_date = (select snapshot_date from target_snapshot)
    group by source_file, upload_dt
)
select
    s.payload,
    s.upload_dt
from source_rows s
join latest_snapshot_file f
    on s.source_file = f.source_file
   and s.upload_dt = f.upload_dt
where s.snapshot_date = (select snapshot_date from target_snapshot)
  and f.rn = 1
