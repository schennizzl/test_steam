{% set snapshot_date = var('snapshot_date', none) %}

with source_rows as (
    select
        cast(appid as bigint) as appid,
        cast(name as varchar) as app_name,
        cast(type as varchar) as app_type,
        cast(is_free as boolean) as is_free,
        cast(required_age as integer) as required_age,
        cast(short_description as varchar) as short_description,
        cast(about_the_game as varchar) as about_the_game,
        cast(supported_languages as varchar) as supported_languages,
        cast(developers as varchar) as developers_json,
        cast(publishers as varchar) as publishers_json,
        cast(website as varchar) as website,
        cast(platform_windows as boolean) as platform_windows,
        cast(platform_mac as boolean) as platform_mac,
        cast(platform_linux as boolean) as platform_linux,
        cast(metacritic_score as integer) as metacritic_score,
        cast(recommendations_total as integer) as recommendations_total,
        cast(release_date as varchar) as release_date_text,
        cast(coming_soon as boolean) as coming_soon,
        cast(price_currency as varchar) as price_currency,
        cast(price_initial as bigint) as price_initial,
        cast(price_final as bigint) as price_final,
        cast(categories_json as varchar) as categories_json,
        cast(genres_json as varchar) as genres_json,
        cast(source_file as varchar) as source_file,
        cast(replace(substr(ingested_at, 1, 19), 'T', ' ') as timestamp(3)) as ingested_at,
        cast(dt as date) as snapshot_date
    from {{ source('landing', 'appdetails_daily_files') }}
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
        ingested_at,
        row_number() over (
            order by ingested_at desc, source_file desc
        ) as rn
    from source_rows
    where snapshot_date = (select snapshot_date from target_snapshot)
    group by source_file, ingested_at
)
select
    s.appid,
    s.app_name,
    s.app_type,
    s.is_free,
    s.required_age,
    s.short_description,
    s.about_the_game,
    s.supported_languages,
    s.developers_json,
    s.publishers_json,
    s.website,
    s.platform_windows,
    s.platform_mac,
    s.platform_linux,
    s.metacritic_score,
    s.recommendations_total,
    s.release_date_text,
    s.coming_soon,
    s.price_currency,
    s.price_initial,
    s.price_final,
    s.categories_json,
    s.genres_json,
    s.source_file,
    s.ingested_at,
    s.snapshot_date
from source_rows s
join latest_snapshot_file f
    on s.source_file = f.source_file
   and s.ingested_at = f.ingested_at
where s.snapshot_date = (select snapshot_date from target_snapshot)
  and f.rn = 1
