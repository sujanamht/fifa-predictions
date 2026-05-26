-- ============================================================
-- mart_head_to_head
-- ============================================================
-- PURPOSE: All-time head-to-head record for every team pair.
--
-- This model answers: "Historically, when Team A plays Team B,
-- who wins more often?"
--
-- KEY DESIGN: We normalize team pairs alphabetically so that
-- (Brazil, Argentina) and (Argentina, Brazil) map to the same row.
-- team_a is always the alphabetically FIRST team.
--
-- This prevents duplicates and makes lookups simple: always
-- query with least(home, away) as team_a and greatest(home, away) as team_b.
-- ============================================================

with matches as (

    select * from {{ ref('stg_match_results') }}

),

-- ---- Normalize pair orientation ----
-- least() / greatest() are PostgreSQL functions that return the
-- smaller / larger of two values alphabetically.
h2h_base as (

    select
        -- Normalized pair (team_a is alphabetically first)
        least(home_team, away_team)                             as team_a,
        greatest(home_team, away_team)                          as team_b,

        match_date,
        tournament,

        -- Did team_a (alphabetically first) win this match?
        case
            when home_team < away_team and result = 'W' then 1  -- home=team_a won
            when home_team > away_team and result = 'L' then 1  -- away=team_a won
            else 0
        end                                                     as team_a_won,

        -- Did team_b (alphabetically second) win?
        case
            when home_team < away_team and result = 'L' then 1  -- home=team_a lost → team_b won
            when home_team > away_team and result = 'W' then 1  -- away=team_a lost → team_b won
            else 0
        end                                                     as team_b_won,

        case when result = 'D' then 1 else 0 end               as is_draw

    from matches

),

-- ---- Aggregate per pair ----
aggregated as (

    select
        team_a,
        team_b,
        count(*)                            as total_matches,
        sum(team_a_won)                     as team_a_wins,
        sum(team_b_won)                     as team_b_wins,
        sum(is_draw)                        as draws,
        min(match_date)                     as first_meeting,
        max(match_date)                     as last_meeting,

        -- Count World Cup matches specifically (more relevant for our prediction)
        sum(case
            when tournament ilike '%FIFA World Cup%'
            then 1 else 0
        end)                                as world_cup_meetings,

        sum(case
            when tournament ilike '%FIFA World Cup%' and team_a_won = 1
            then 1 else 0
        end)                                as team_a_wc_wins,

        sum(case
            when tournament ilike '%FIFA World Cup%' and team_b_won = 1
            then 1 else 0
        end)                                as team_b_wc_wins

    from h2h_base
    group by team_a, team_b

)

select
    team_a,
    team_b,
    total_matches,
    team_a_wins,
    team_b_wins,
    draws,

    -- Win rate for team_a — used directly as an ML feature
    round(
        team_a_wins::numeric / nullif(total_matches, 0),
        3
    )                                       as team_a_win_rate,

    round(
        team_b_wins::numeric / nullif(total_matches, 0),
        3
    )                                       as team_b_win_rate,

    round(
        draws::numeric / nullif(total_matches, 0),
        3
    )                                       as draw_rate,

    world_cup_meetings,
    team_a_wc_wins,
    team_b_wc_wins,
    first_meeting,
    last_meeting

from aggregated

-- Only include pairs that have actually played each other
where total_matches >= 1

order by total_matches desc
