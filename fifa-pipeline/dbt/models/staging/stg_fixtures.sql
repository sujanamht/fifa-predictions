-- ============================================================
-- stg_fixtures
-- ============================================================
-- PURPOSE: Clean and expose the 2026 WC group stage fixture list.
--
-- SOURCE: wc2026_fixtures seed (dbt/seeds/wc2026_fixtures.csv)
--
-- HOW TO UPDATE FIXTURES:
--   Edit dbt/seeds/wc2026_fixtures.csv and run:
--     dbt seed --select wc2026_fixtures
--   This re-loads the CSV into Postgres as a table.
--
-- NOTE: The seed contains 48 illustrative group stage fixtures
--       across 8 groups. Update with official match dates and
--       venues once the official schedule is confirmed.
-- ============================================================

with source as (

    -- {{ ref() }} references a dbt seed — dbt tracks this as a dependency
    -- so running dbt run rebuilds this model if the seed changes.
    select * from {{ ref('wc2026_fixtures') }}

),

cleaned as (

    select
        fixture_id,
        upper(trim(group_name))     as group_name,   -- Normalize to uppercase: 'a' → 'A'
        trim(home_team)             as home_team,
        trim(away_team)             as away_team,
        match_date::date            as match_date,
        trim(venue)                 as venue,
        trim(city)                  as city,

        -- Convenience flag: is this a local derby (same confederation)?
        -- Useful as a feature — derby matches tend to be tighter
        false                       as is_derby       -- Placeholder; can be populated later

    from source

    where
        -- Basic sanity checks
        fixture_id is not null
        and home_team is not null
        and away_team is not null
        and match_date is not null

)

select * from cleaned
order by match_date, group_name, fixture_id
