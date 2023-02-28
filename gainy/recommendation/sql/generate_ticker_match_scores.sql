insert into app.profile_ticker_match_score (profile_id, symbol, match_score, fits_risk, risk_similarity,
                                            fits_categories, fits_interests, category_matches, interest_matches,
                                            updated_at, category_similarity, interest_similarity, matches_portfolio)
with profiles as materialized
         (
             select id as profile_id, email
             from app.profiles
                      join app.profile_scoring_settings on profiles.id = profile_scoring_settings.profile_id
             {where_clause}
         ),
     tickers as materialized
         (
             select symbol
             from tickers
             where tickers.type in ('preferred stock', 'common stock', 'crypto', 'etf')
               and tickers.ms_enabled
     ),
     p_rsk as
         (
             select profile_id, risk_score, (risk_score::double precision - 1) / 2 as value
             from profiles
                      join app.profile_scoring_settings using (profile_id)
         ),
     p_cat as
         (
             select profile_id, category_id
             from profiles
                      join app.profile_categories using (profile_id)
         ),
     p_int as
         (
             select profile_id, interest_id
             from profiles
                      join app.profile_interests using (profile_id)
         ),
     t_cat_sim_dif as
         (
             select category_id, symbol, sim_dif
             from ticker_categories_continuous
                      join tickers using (symbol)
         ),
     t_int_sim_dif as
         (
             select interest_id, symbol, sim_dif
             from ticker_interests
                      join tickers using (symbol)
         ),
     t_risk_score as
         (
             select symbol, risk_score
             from ticker_risk_scores
                      join tickers using (symbol)
         ),
     const as
         (
             select *
             from (values (3.8, 6.53, 3.38)) t ("d", "sr", "sc")
         ),
     risk_similarity as
         (
             select profile_id,
                    symbol,
                    case
                        when p_rsk.risk_score = 3
                            then 1 - abs(t_risk_score.risk_score - 0.75)
                        when p_rsk.risk_score = 2
                            then 1 - abs(t_risk_score.risk_score - 0.5) * 1.5
                        when p_rsk.risk_score = 1
                            then 1 - abs(t_risk_score.risk_score - 0.25)
                        end as match_comp_risk
             from profiles
                      join p_rsk using (profile_id)
                      join const on true
                      left join t_risk_score on true
         ),
     category_similarity as
         (
             select profile_id,
                    symbol,
                    coalesce(max(sim_dif), -1) as match_comp_category
             from profiles
                      left join p_cat using (profile_id)
                      left join tickers on true
                      left join t_cat_sim_dif using (category_id, symbol)
             group by profile_id, symbol
         ),
     category_matches as
         (
             select profile_id,
                    symbol,
                    json_agg(category_id) as category_matches
             from (
                      select profile_id,
                             symbol,
                             category_id,
                             sim_dif,
                             row_number()
                             over (partition by profile_id, symbol order by sim_dif desc nulls last) as row_number
                      from profiles
                               left join p_cat using (profile_id)
                               left join tickers on true
                               left join t_cat_sim_dif using (category_id, symbol)
                      order by profile_id, symbol, sim_dif desc nulls last
                  ) t
             where row_number <= 2
               and sim_dif is not null
               and sim_dif > 0
             group by profile_id, symbol
         ),
     interest_similarity as
         (
             select profile_id,
                    symbol,
                    coalesce(max(sim_dif), -1) as match_comp_interest
             from profiles
                      left join p_int using (profile_id)
                      left join tickers on true
                      left join t_int_sim_dif using (interest_id, symbol)
             group by profile_id, symbol
         ),
     interest_matches as
         (
             select profile_id,
                    symbol,
                    json_agg(interest_id) as interest_matches
             from (
                      select profile_id,
                             symbol,
                             interest_id,
                             sim_dif,
                             row_number()
                             over (partition by profile_id, symbol order by sim_dif desc nulls last) as row_number
                      from profiles
                               left join p_int using (profile_id)
                               left join tickers on true
                               left join t_int_sim_dif using (interest_id, symbol)
                      order by profile_id, symbol, sim_dif desc nulls last
                  ) t
             where row_number <= 2
               and sim_dif is not null
               and sim_dif > 0
             group by profile_id, symbol
         ),
     portfolio_interest_similarity as
         (
             select profile_id,
                    symbol,
                    max(sim_dif + (1. - sim_dif) *
                                  pow(abs((1. - sim_dif) / 2. * (1. - (1. - sim_dif) / 2.)), 0.5)) as match_comp_interest
             from ticker_interests
                      join (
                               select profile_id,
                                      interest_id
                               from profiles
                                        join app.profile_holdings ph using (profile_id)
                                        join app.portfolio_securities ps on ps.id = ph.security_id
                                        join ticker_interests tint on tint.symbol = ps.ticker_symbol
                               group by interest_id, profile_id
                           ) t using (interest_id)
             group by profile_id, symbol
         ),
     combined0 as (
         select profile_id,
                symbol,
                coalesce(match_comp_risk, 0)                                   as match_comp_risk_normalized,
                coalesce(match_comp_category / 2 + 0.5, 0)                     as match_comp_category_normalized,
                coalesce(interest_similarity.match_comp_interest / 2 + 0.5, 0) as match_comp_interest_normalized,
                coalesce(category_matches::text, '[]')                         as category_matches,
                coalesce(interest_matches::text, '[]')                         as interest_matches,
                portfolio_interest_similarity.match_comp_interest is not null  as matches_portfolio
         from profiles
                  join tickers on true
                  left join risk_similarity using (profile_id, symbol)
                  left join category_similarity using (profile_id, symbol)
                  left join interest_similarity using (profile_id, symbol)
                  left join portfolio_interest_similarity using (profile_id, symbol)
                  left join category_matches using (profile_id, symbol)
                  left join interest_matches using (profile_id, symbol)
     )
select profile_id,
       symbol,
       ((public.sigmoid(match_comp_risk_normalized * 0.6, 3) +
       public.sigmoid(match_comp_interest_normalized * 0.3, 3) +
       public.sigmoid(match_comp_category_normalized * 0.1, 3)) * 100)::int                        as match_score,
       (match_comp_risk_normalized > 1/3.)::int + (match_comp_risk_normalized > 2/3.)::int         as fits_risk,
       match_comp_risk_normalized                                                                  as risk_similarity,
       (match_comp_category_normalized > 1/3.)::int + (match_comp_category_normalized > 2/3.)::int as fits_categories,
       (match_comp_interest_normalized > 1/3.)::int + (match_comp_interest_normalized > 2/3.)::int as fits_interests,
       category_matches,
       interest_matches,
       now()                                                                                       as updated_at,
       match_comp_category_normalized                                                              as category_similarity,
       match_comp_interest_normalized                                                              as interest_similarity,
       matches_portfolio
from combined0
on conflict (
    profile_id, symbol
    ) do update set match_score         = excluded.match_score,
                    fits_risk           = excluded.fits_risk,
                    risk_similarity     = excluded.risk_similarity,
                    fits_categories     = excluded.fits_categories,
                    fits_interests      = excluded.fits_interests,
                    category_matches    = excluded.category_matches,
                    interest_matches    = excluded.interest_matches,
                    updated_at          = excluded.updated_at,
                    category_similarity = excluded.category_similarity,
                    interest_similarity = excluded.interest_similarity,
                    matches_portfolio   = excluded.matches_portfolio;
