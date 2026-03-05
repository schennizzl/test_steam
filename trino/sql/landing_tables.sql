CREATE SCHEMA IF NOT EXISTS hive.landing
WITH (location = 's3a://raw/steam/landing/');

CREATE SCHEMA IF NOT EXISTS hive.raw
WITH (location = 's3a://raw/warehouse/raw/');

CREATE SCHEMA IF NOT EXISTS hive.stg
WITH (location = 's3a://raw/warehouse/stg/');

CREATE TABLE IF NOT EXISTS hive.landing.store_daily_files (
    appid BIGINT,
    name VARCHAR,
    type VARCHAR,
    source_file VARCHAR,
    ingested_at VARCHAR,
    dt DATE
)
WITH (
    external_location = 's3a://raw/steam/landing/store_daily/',
    format = 'JSON',
    partitioned_by = ARRAY['dt']
);

CREATE TABLE IF NOT EXISTS hive.landing.appdetails_types_files (
    appid BIGINT,
    name VARCHAR,
    type VARCHAR,
    source_file VARCHAR,
    ingested_at VARCHAR,
    dt DATE,
    hour INTEGER
)
WITH (
    external_location = 's3a://raw/steam/landing/appdetails_types/',
    format = 'JSON',
    partitioned_by = ARRAY['dt', 'hour']
);

CREATE TABLE IF NOT EXISTS hive.landing.appdetails_daily_files (
    appid BIGINT,
    name VARCHAR,
    type VARCHAR,
    is_free BOOLEAN,
    required_age INTEGER,
    short_description VARCHAR,
    about_the_game VARCHAR,
    supported_languages VARCHAR,
    developers VARCHAR,
    publishers VARCHAR,
    website VARCHAR,
    platform_windows BOOLEAN,
    platform_mac BOOLEAN,
    platform_linux BOOLEAN,
    metacritic_score INTEGER,
    recommendations_total INTEGER,
    release_date VARCHAR,
    coming_soon BOOLEAN,
    price_currency VARCHAR,
    price_initial BIGINT,
    price_final BIGINT,
    categories_json VARCHAR,
    genres_json VARCHAR,
    source_file VARCHAR,
    ingested_at VARCHAR,
    dt DATE
)
WITH (
    external_location = 's3a://raw/steam/landing/appdetails_daily/',
    format = 'JSON',
    partitioned_by = ARRAY['dt']
);

CREATE TABLE IF NOT EXISTS hive.landing.game_online_files (
    appid BIGINT,
    game_name VARCHAR,
    current_players INTEGER,
    source_file VARCHAR,
    ingested_at VARCHAR,
    dt DATE,
    hour INTEGER
)
WITH (
    external_location = 's3a://raw/steam/landing/game_online/',
    format = 'JSON',
    partitioned_by = ARRAY['dt', 'hour']
);

CREATE TABLE IF NOT EXISTS hive.landing.twitch_viewers_files (
    appid BIGINT,
    game_name VARCHAR,
    twitch_lookup_name VARCHAR,
    twitch_category_id VARCHAR,
    twitch_category_name VARCHAR,
    twitch_viewers INTEGER,
    approx_total_viewers INTEGER,
    live_channels INTEGER,
    pages_fetched INTEGER,
    is_partial BOOLEAN,
    source_file VARCHAR,
    ingested_at VARCHAR,
    dt DATE,
    hour INTEGER
)
WITH (
    external_location = 's3a://raw/steam/landing/twitch_viewers/',
    format = 'JSON',
    partitioned_by = ARRAY['dt', 'hour']
);

CREATE TABLE IF NOT EXISTS hive.landing.twitch_channels_files (
    appid BIGINT,
    game_name VARCHAR,
    twitch_lookup_name VARCHAR,
    configured_twitch_category_id VARCHAR,
    twitch_category_id VARCHAR,
    twitch_category_name VARCHAR,
    broadcaster_id VARCHAR,
    broadcaster_login VARCHAR,
    broadcaster_name VARCHAR,
    title VARCHAR,
    language VARCHAR,
    started_at VARCHAR,
    thumbnail_url VARCHAR,
    is_mature BOOLEAN,
    viewer_count INTEGER,
    pages_fetched INTEGER,
    is_partial BOOLEAN,
    source_file VARCHAR,
    ingested_at VARCHAR,
    dt DATE,
    hour INTEGER
)
WITH (
    external_location = 's3a://raw/steam/landing/twitch_channels/',
    format = 'JSON',
    partitioned_by = ARRAY['dt', 'hour']
);
