-- ============================================================
-- stg_match_results
-- ============================================================
-- PURPOSE: Clean and standardize raw international match results.
--
-- SOURCE: raw_results table (loaded by notebook 01_ingestion.ipynb)
--
-- TRANSFORMATIONS:
--   - Cast date string to DATE type
--   - Cast neutral string to BOOLEAN
--   - Derive result column: W / D / L from home team perspective
--   - Add goal_diff for convenience
--   - Drop rows with null scores (unusable for ML)
-- ============================================================

with source as (

    -- Reference the raw table using dbt's {{ source() }} macro.
    -- This links the model to the source defined in schema.yml,
    -- enabling data lineage tracking in dbt docs.
    select * from {{ source('raw_data', 'raw_results') }}

),

cleaned as (

    select
        -- Parse date: stored as VARCHAR 'YYYY-MM-DD' in raw table
        date::date                              as match_date,

        trim(home_team)                         as home_team,
        trim(away_team)                         as away_team,

        home_score::integer                     as home_score,
        away_score::integer                     as away_score,

        trim(tournament)                        as tournament,
        trim(city)                              as city,
        trim(country)                           as country,

        -- Convert 'TRUE'/'FALSE' string to boolean
        case
            when upper(neutral) = 'TRUE'  then true
            when upper(neutral) = 'FALSE' then false
            else false
        end                                     as is_neutral,

        -- Match result from HOME TEAM perspective (our ML target variable)
        case
            when home_score::integer > away_score::integer  then 'W'
            when home_score::integer = away_score::integer  then 'D'
            else                                                 'L'
        end                                     as result,

        -- Goal difference: positive = home team dominated
        home_score::integer - away_score::integer   as goal_diff

    from source

    where
        -- Drop rows with missing scores — cannot derive result or goal features
        home_score is not null
        and away_score is not null
        and home_team  is not null
        and away_team  is not null
        and date       is not null

        -- Drop rows with implausible negative scores (data entry errors)
        and home_score::integer >= 0
        and away_score::integer >= 0

)

select * from cleaned
