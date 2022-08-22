insert into app.profile_collection_match_score (profile_id, collection_uniq_id, match_score, risk_similarity,
                                                category_similarity, interest_similarity, updated_at, risk_level,
                                                category_level, interest_level)
with profiles as
         (
             select id as profile_id, email
             from app.profiles
             {where_clause}
         )
select t.profile_id,
       t.collection_uniq_id,
       t.match_score,
       t.risk_similarity,
       t.category_similarity,
       t.interest_similarity,
       now()                                                               as updated_at,
       (risk_similarity > 0.3)::int + (risk_similarity > 0.7)::int         as risk_level,
       (category_similarity > 0.3)::int + (category_similarity > 0.7)::int as category_level,
       (interest_similarity > 0.3)::int + (interest_similarity > 0.7)::int as interest_level
from (
         select profile_ticker_match_score.profile_id,
                collection_uniq_id,
                (sum(match_score * weight) / sum(weight))::double precision         as match_score,
                (sum(risk_similarity * weight) / sum(weight))::double precision     as risk_similarity,
                (sum(category_similarity * weight) / sum(weight))::double precision as category_similarity,
                (sum(interest_similarity * weight) / sum(weight))::double precision as interest_similarity
         from profiles
                  join app.profile_ticker_match_score using (profile_id)
                  join collection_tickers_weighted
                       on (collection_tickers_weighted.profile_id is null or
                           collection_tickers_weighted.profile_id = profiles.profile_id)
                           and profile_ticker_match_score.symbol = collection_tickers_weighted.symbol
         group by profile_ticker_match_score.profile_id, collection_uniq_id
         having sum(weight) > 0
     ) t
on conflict (
    profile_id, collection_uniq_id
    ) do update set profile_id          = excluded.profile_id,
                    collection_uniq_id  = excluded.collection_uniq_id,
                    match_score         = excluded.match_score,
                    risk_similarity     = excluded.risk_similarity,
                    category_similarity = excluded.category_similarity,
                    interest_similarity = excluded.interest_similarity,
                    updated_at          = excluded.updated_at,
                    risk_level          = excluded.risk_level,
                    category_level      = excluded.category_level,
                    interest_level      = excluded.interest_level;
