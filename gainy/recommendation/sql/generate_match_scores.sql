insert into app.profile_ticker_match_score (profile_id, symbol, match_score, fits_risk, risk_similarity,
                                            fits_categories, fits_interests, category_matches, interest_matches,
                                            updated_at, category_similarity, interest_similarity, portfolio_matches)
with p_rsk as
         (
             select profile_id, (risk_score::double precision - 1) / 2 as value
             from app.profile_scoring_settings
         ),
     p_cat as
         (
             select profile_id, category_id
             from app.profile_categories
         ),
     p_int as
         (
             select profile_id, interest_id
             from app.profile_interests
         ),
     t_cat_sim_dif as
         (
             select category_id, symbol, sim_dif
             from ticker_categories_continuous
         ),
     t_int_sim_dif as
         (
             select interest_id, symbol, sim_dif
             from ticker_interests
         ),
     t_risk_score as
         (
             select symbol, risk_score
             from ticker_risk_scores
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
                    1. / (1. + pow(abs(p_rsk.value - coalesce(t_risk_score.risk_score, 0.5)), d) *
                               pow(abs(sr + (sc - sr) * abs(p_rsk.value - 0.5) / 0.5), d)) * 2 - 1 as match_comp_risk
             from app.profiles
                      join const on true
                      left join p_rsk on p_rsk.profile_id = profiles.id
                      left join tickers on true
                      left join t_risk_score using (symbol)
         ),
     category_similarity as
         (
             select profile_id,
                    symbol,
                    coalesce(max(sim_dif), -1) as match_comp_category
             from app.profiles
                      left join p_cat on p_cat.profile_id = profiles.id
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
                      from app.profiles
                               left join p_cat on p_cat.profile_id = profiles.id
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
             from app.profiles
                      left join p_int on p_int.profile_id = profiles.id
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
                      from app.profiles
                               left join p_int on p_int.profile_id = profiles.id
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
                    max(tint.sim_dif + (1. - tint.sim_dif) *
                                       pow(abs((1. - tint.sim_dif) / 2. * (1. - (1. - tint.sim_dif) / 2.)),
                                           0.5)) as match_comp_interest
             from app.profile_holdings ph
                      join app.portfolio_securities ps on ps.id = ph.security_id
                      join ticker_interests tint on tint.symbol = ps.ticker_symbol
             group by profile_id, symbol
         ),
     combined0 as (
         select profile_id,
                symbol,
                (match_comp_risk + match_comp_category +
                 greatest(interest_similarity.match_comp_interest, portfolio_interest_similarity.match_comp_interest)) /
                3 / 2 + 0.5                                                   as match_score,
                match_comp_risk / 2 + 0.5                                     as match_comp_risk_normalized,
                match_comp_category / 2 + 0.5                                 as match_comp_category_normalized,
                greatest(interest_similarity.match_comp_interest, portfolio_interest_similarity.match_comp_interest) /
                2 + 0.5                                                       as match_comp_interest_normalized,
                coalesce(category_matches::text, '[]')                        as category_matches,
                coalesce(interest_matches::text, '[]')                        as interest_matches,
                portfolio_interest_similarity.match_comp_interest is not null as portfolio_matches
         from risk_similarity
                  left join category_similarity using (profile_id, symbol)
                  left join interest_similarity using (profile_id, symbol)
                  left join portfolio_interest_similarity using (profile_id, symbol)
                  left join category_matches using (profile_id, symbol)
                  left join interest_matches using (profile_id, symbol) {where_clause}
     ),
     combined1 as (
         select profile_id,
                symbol,
                (case
                     when match_score - 0.5 > 0
                         then (match_score - 0.5) / max_match_score
                     when match_score - 0.5 < 0
                         then (match_score - 0.5) / -min_match_score
                     else match_score
                     end + 1) / 2 as match_score,
                match_comp_risk_normalized,
                match_comp_category_normalized,
                match_comp_interest_normalized,
                category_matches,
                interest_matches,
                portfolio_matches
         from combined0
                  join (
             select profile_id,
                    max(match_score) as max_match_score,
                    min(match_score) as min_match_score
             from combined0
             group by profile_id
         ) t using (profile_id)
     )
select profile_id,
       symbol,
       (match_score * 100)::int                                                                  as match_score,
       (match_comp_risk_normalized > 0.3)::int + (match_comp_risk_normalized > 0.7)::int         as fits_risk,
       match_comp_risk_normalized                                                                as risk_similarity,
       (match_comp_category_normalized > 0.3)::int + (match_comp_category_normalized > 0.7)::int as fits_categories,
       (match_comp_interest_normalized > 0.3)::int + (match_comp_interest_normalized > 0.7)::int as fits_interests,
       category_matches,
       interest_matches,
       now()                                                                                     as updated_at,
       match_comp_category_normalized                                                            as category_similarity,
       match_comp_interest_normalized                                                            as interest_similarity,
       portfolio_matches
from combined1
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
                    interest_similarity = excluded.interest_similarity;