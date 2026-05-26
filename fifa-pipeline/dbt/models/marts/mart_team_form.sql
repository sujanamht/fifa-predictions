-- ============================================================
-- mart_team_form
-- ============================================================
-- PURPOSE: Aggregated team form metrics for every team.
--
-- This model answers: "Right now, how is each team performing?"
--
-- METRICS:
--   last10  = last 10 matches (short-term momentum)
--   last20  = last 20 matches (medium-term strength)
--
-- HOW IT WORKS:
--   1. "Unpivot" matches: home view + away view = 2 rows per match
--   2. For each team, rank matches newest-first
--   3. Aggregate over the top 10 / top 20 ranked rows
--
-- NOTE: This uses a window function (row_number) + conditional aggregation.
--       It's equivalent to the PySpark window work in notebook 03,
--       but expressed in pure SQL — showing dbt's power as a transformation
--       layer on top of Postgres.
-- ============================================================

with all_matches as (

    -- ---- Create team-centric view ----
    -- Each match appears TWICE: once for home team, once for away team.
    -- This lets us aggregate stats per team regardless of home/away venue.

    -- Home team perspective
    select
        match_date,
        home_team                                       as team,
        away_team                                       as opponent,
        home_score                                      as goals_for,
        away_score                                      as goals_against,
        result                                          as match_result,
        goal_diff,
        'home'                                          as venue
    from {{ ref('stg_match_results') }}

    union all

    -- Away team perspective (flip W/L — away win = home 'L')
    select
        match_date,
        away_team                                       as team,
        home_team                                       as opponent,
        away_score                                      as goals_for,
        home_score                                      as goals_against,
        case result
            when 'W' then 'L'    -- Home win → away loss
            when 'L' then 'W'    -- Home loss → away win
            else 'D'
        end                                             as match_result,
        away_score - home_score                         as goal_diff,
        'away'                                          as venue
    from {{ ref('stg_match_results') }}

),

-- ---- Rank each team's matches by recency ----
-- rank=1 is the most recent match, rank=20 is the 20th most recent
ranked as (
    select
        *,
        row_number() over (
            partition by team
            order by match_date desc
        )                                               as match_rank
    from all_matches
),

-- ---- Last 10 matches stats ----
form_last10 as (
    select
        team,
        count(*)                                                        as matches_last10,
        sum(case when match_result = 'W' then 1 else 0 end)            as wins_last10,
        sum(case when match_result = 'D' then 1 else 0 end)            as draws_last10,
        sum(case when match_result = 'L' then 1 else 0 end)            as losses_last10,
        round(avg(goals_for)::numeric, 2)                              as avg_goals_scored_last10,
        round(avg(goals_against)::numeric, 2)                          as avg_goals_conceded_last10,
        round(avg(goal_diff)::numeric, 2)                              as avg_goal_diff_last10,
        -- Win rate: what fraction of last 10 matches did they win?
        round(
            sum(case when match_result = 'W' then 1 else 0 end)::numeric
            / nullif(count(*), 0),
            3
        )                                                               as win_rate_last10,
        max(match_date)                                                 as last_match_date
    from ranked
    where match_rank <= 10
    group by team
),

-- ---- Last 20 matches stats (broader view) ----
form_last20 as (
    select
        team,
        count(*)                                                        as matches_last20,
        sum(case when match_result = 'W' then 1 else 0 end)            as wins_last20,
        round(avg(goals_for)::numeric, 2)                              as avg_goals_scored_last20,
        round(avg(goals_against)::numeric, 2)                          as avg_goals_conceded_last20
    from ranked
    where match_rank <= 20
    group by team
),

-- ---- Trend: compare last 5 vs previous 5 ----
-- A positive trend means the team's GD is improving
form_last5 as (
    select
        team,
        round(avg(goal_diff)::numeric, 2)  as avg_gd_last5
    from ranked
    where match_rank <= 5
    group by team
),

form_prev5 as (
    select
        team,
        round(avg(goal_diff)::numeric, 2)  as avg_gd_prev5
    from ranked
    where match_rank between 6 and 10
    group by team
)

-- ---- Final join ----
select
    f10.team,
    f10.last_match_date,
    f10.matches_last10,
    f10.wins_last10,
    f10.draws_last10,
    f10.losses_last10,
    f10.win_rate_last10,
    f10.avg_goals_scored_last10,
    f10.avg_goals_conceded_last10,
    f10.avg_goal_diff_last10,

    f20.matches_last20,
    f20.wins_last20,
    f20.avg_goals_scored_last20,
    f20.avg_goals_conceded_last20,

    -- Goal diff trend: positive = improving form
    round(
        coalesce(f5.avg_gd_last5, 0) - coalesce(fp5.avg_gd_prev5, 0),
        3
    )                                       as goal_diff_trend

from form_last10 f10
left join form_last20  f20  on f10.team = f20.team
left join form_last5   f5   on f10.team = f5.team
left join form_prev5   fp5  on f10.team = fp5.team

order by f10.win_rate_last10 desc
