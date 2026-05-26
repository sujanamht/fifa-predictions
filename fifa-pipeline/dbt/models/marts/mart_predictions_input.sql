-- ============================================================
-- mart_predictions_input
-- ============================================================
-- PURPOSE: Final ML-ready table joining 2026 WC fixtures with
--          all predictive features for both teams.
--
-- This is what the ML model reads in Phase 4.
--
-- JOINS:
--   fixtures → home team form → away team form → h2h → rankings
--
-- EACH ROW = one 2026 WC group stage match + all features needed
--            to predict the outcome.
-- ============================================================

with fixtures as (

    select * from {{ ref('stg_fixtures') }}

),

team_form as (

    select * from {{ ref('mart_team_form') }}

),

h2h as (

    select * from {{ ref('mart_head_to_head') }}

),

rankings as (

    select * from {{ ref('stg_rankings') }}

),

-- ---- Join HOME team features ----
with_home_features as (

    select
        f.fixture_id,
        f.group_name,
        f.home_team,
        f.away_team,
        f.match_date,
        f.venue,
        f.city,

        -- Home team form
        coalesce(tf.win_rate_last10,           0.33)    as home_win_rate_last10,
        coalesce(tf.avg_goals_scored_last10,   1.2)     as home_avg_goals_scored,
        coalesce(tf.avg_goals_conceded_last10, 1.2)     as home_avg_goals_conceded,
        coalesce(tf.avg_goal_diff_last10,      0.0)     as home_avg_goal_diff,
        coalesce(tf.goal_diff_trend,           0.0)     as home_goal_diff_trend,
        coalesce(tf.matches_last10,            0)       as home_matches_available,

        -- Home team ranking
        coalesce(r.strength_score,  50.0)               as home_strength_score,
        coalesce(r.derived_rank,    100)                as home_derived_rank,
        coalesce(r.avg_goals_scored, 1.2)               as home_rank_avg_goals

    from fixtures f
    left join team_form tf on f.home_team = tf.team
    left join rankings  r  on f.home_team = r.team

),

-- ---- Join AWAY team features ----
with_away_features as (

    select
        wh.*,

        -- Away team form
        coalesce(tf.win_rate_last10,           0.33)    as away_win_rate_last10,
        coalesce(tf.avg_goals_scored_last10,   1.2)     as away_avg_goals_scored,
        coalesce(tf.avg_goals_conceded_last10, 1.2)     as away_avg_goals_conceded,
        coalesce(tf.avg_goal_diff_last10,      0.0)     as away_avg_goal_diff,
        coalesce(tf.goal_diff_trend,           0.0)     as away_goal_diff_trend,
        coalesce(tf.matches_last10,            0)       as away_matches_available,

        -- Away team ranking
        coalesce(r.strength_score,  50.0)               as away_strength_score,
        coalesce(r.derived_rank,    100)                as away_derived_rank,
        coalesce(r.avg_goals_scored, 1.2)               as away_rank_avg_goals

    from with_home_features wh
    left join team_form tf on wh.away_team = tf.team
    left join rankings  r  on wh.away_team = r.team

),

-- ---- Join H2H stats ----
-- H2H uses normalized pairs: join on least/greatest of team names
with_h2h as (

    select
        wa.*,

        coalesce(h.total_matches,     0)                as h2h_total_matches,
        coalesce(h.team_a_wins,       0)                as h2h_team_a_wins,
        coalesce(h.team_b_wins,       0)                as h2h_team_b_wins,
        coalesce(h.draws,             0)                as h2h_draws,
        coalesce(h.world_cup_meetings, 0)               as h2h_wc_meetings,

        -- H2H win rate FROM HOME TEAM'S PERSPECTIVE
        -- If home_team is alphabetically first (= team_a), use team_a_win_rate
        -- Otherwise use team_b_win_rate
        coalesce(
            case
                when least(wa.home_team, wa.away_team) = wa.home_team
                then h.team_a_win_rate   -- home team is team_a
                else h.team_b_win_rate   -- home team is team_b
            end,
            0.33   -- Default = equal chance (1/3) if no history
        )                                               as home_h2h_win_rate,

        coalesce(h.draw_rate, 0.25)                    as h2h_draw_rate

    from with_away_features wa
    left join h2h h on (
        least(wa.home_team,    wa.away_team) = h.team_a
        and greatest(wa.home_team, wa.away_team) = h.team_b
    )

),

-- ---- Compute derived features ----
final as (

    select
        *,

        -- Relative strength: positive = home team is stronger
        home_strength_score - away_strength_score       as strength_diff,

        -- Ranking difference: positive = home team ranks higher (lower rank number = better)
        away_derived_rank - home_derived_rank           as rank_diff,

        -- Goal scoring differential
        home_avg_goals_scored - away_avg_goals_conceded as home_attack_vs_away_defence,
        away_avg_goals_scored - home_avg_goals_conceded as away_attack_vs_home_defence

    from with_h2h

)

select * from final
order by match_date, group_name, fixture_id
