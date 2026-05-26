-- ============================================================
-- stg_rankings
-- ============================================================
-- PURPOSE: Derive a team strength score (ranking proxy) from
--          recent match results.
--
-- WHY WE DERIVE THIS:
--   The dataset does not include official FIFA rankings.
--   We compute a "strength score" using the last 12 months of
--   results: (wins*3 + draws) / (total_matches * 3) * 100.
--   This mirrors the FIFA points formula and correlates strongly
--   with actual rankings.
--
--   If you obtain a real FIFA rankings CSV, load it as a raw_rankings
--   table in Postgres and replace this model with a simple select.
-- ============================================================

with recent_matches as (

    select * from {{ ref('stg_match_results') }}

    -- Only the last 12 months — ranking should reflect current form,
    -- not a team's results from 30 years ago
    where match_date >= current_date - interval '365 days'

),

-- Build a home-team perspective row per match
home_stats as (
    select
        home_team                                           as team,
        count(*)                                            as matches,
        sum(case when result = 'W' then 1 else 0 end)      as wins,
        sum(case when result = 'D' then 1 else 0 end)      as draws,
        sum(case when result = 'L' then 1 else 0 end)      as losses,
        round(avg(home_score)::numeric, 2)                  as avg_goals_scored,
        round(avg(away_score)::numeric, 2)                  as avg_goals_conceded
    from recent_matches
    group by home_team
),

-- Build an away-team perspective row per match
away_stats as (
    select
        away_team                                           as team,
        count(*)                                            as matches,
        -- Away win = result is 'L' from home team's perspective
        sum(case when result = 'L' then 1 else 0 end)      as wins,
        sum(case when result = 'D' then 1 else 0 end)      as draws,
        sum(case when result = 'W' then 1 else 0 end)      as losses,
        round(avg(away_score)::numeric, 2)                  as avg_goals_scored,
        round(avg(home_score)::numeric, 2)                  as avg_goals_conceded
    from recent_matches
    group by away_team
),

-- Combine home and away stats per team
combined as (
    select
        coalesce(h.team, a.team)                            as team,
        coalesce(h.matches, 0) + coalesce(a.matches, 0)    as total_matches,
        coalesce(h.wins,    0) + coalesce(a.wins,    0)    as total_wins,
        coalesce(h.draws,   0) + coalesce(a.draws,   0)    as total_draws,
        coalesce(h.losses,  0) + coalesce(a.losses,  0)    as total_losses,
        -- Weighted average goals: combine home + away perspectives
        round(
            (
                coalesce(h.avg_goals_scored,    0) * coalesce(h.matches, 0)
                + coalesce(a.avg_goals_scored,  0) * coalesce(a.matches, 0)
            )::numeric
            / nullif(coalesce(h.matches, 0) + coalesce(a.matches, 0), 0),
            2
        )                                                   as avg_goals_scored,
        round(
            (
                coalesce(h.avg_goals_conceded,  0) * coalesce(h.matches, 0)
                + coalesce(a.avg_goals_conceded,0) * coalesce(a.matches, 0)
            )::numeric
            / nullif(coalesce(h.matches, 0) + coalesce(a.matches, 0), 0),
            2
        )                                                   as avg_goals_conceded
    from home_stats h
    full outer join away_stats a on h.team = a.team
),

-- Compute strength score and rank
ranked as (
    select
        team,
        total_matches,
        total_wins,
        total_draws,
        total_losses,
        avg_goals_scored,
        avg_goals_conceded,

        -- Strength score: FIFA-style points percentage (0–100)
        -- A team winning every game scores 100; losing every game scores 0
        round(
            (total_wins * 3.0 + total_draws)
            / nullif(total_matches * 3.0, 0)
            * 100,
            2
        )                                                   as strength_score,

        -- Rank all teams by strength score (1 = strongest)
        rank() over (
            order by
                (total_wins * 3.0 + total_draws)
                / nullif(total_matches * 3.0, 0) desc
        )                                                   as derived_rank

    from combined
    -- Require at least 5 matches to produce a meaningful score
    where total_matches >= 5
)

select * from ranked
