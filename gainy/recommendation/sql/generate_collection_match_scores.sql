insert into app.profile_collection_match_score (profile_id, collection_id, collection_uniq_id, match_score, risk_similarity,
                                                category_similarity, interest_similarity, updated_at, risk_level,
                                                category_level, interest_level)
with profiles as
         (
             select id as profile_id, email
             from app.profiles
             {where_clause}
         ),
risk_similarity as
         (
             select profile_id,
                    collection_uniq_id,
                    collection_id,
                    case
                        when profile_scoring_settings.risk_score = 3
                            then 1 - abs(collection_risk_scores.risk_score - 0.75)
                        when profile_scoring_settings.risk_score = 2
                            then 1 - abs(collection_risk_scores.risk_score - 0.5) * 1.5
                        when profile_scoring_settings.risk_score = 1
                            then 1 - abs(collection_risk_scores.risk_score - 0.25)
                        end as risk_similarity
             from profiles
                      join app.profile_scoring_settings using (profile_id)
                      left join collection_risk_scores on true
     ),
     cat_eligibility as
         (
             select distinct collection_id
             from collection_categories
             where sim_dif > 0.5
     ),
     int_eligibility as
         (
             select distinct collection_id
             from collection_interests
             where sim_dif > 0.5
     ),
     cat_int_similarity as
         (
             select profiles.profile_id,
                    collection_uniq_id,
                    (sum(category_similarity * weight) / sum(weight))::double precision as category_similarity,
                    (sum(interest_similarity * weight) / sum(weight))::double precision as interest_similarity
             from profiles
                      join collection_ticker_actual_weights on true
                      join app.profile_ticker_match_score
                           on profile_ticker_match_score.profile_id = profiles.profile_id and
                              profile_ticker_match_score.symbol = collection_ticker_actual_weights.symbol
             group by profiles.profile_id, collection_uniq_id
             having sum(weight) > 0
     )
select profile_id,
       collection_id,
       collection_uniq_id,
       (case
            when cat_eligibility.collection_id is not null and int_eligibility.collection_id is not null
                then public.sigmoid(risk_similarity, 3) * 0.6 +
                     public.sigmoid(interest_similarity, 3) * 0.3 +
                     public.sigmoid(category_similarity, 3) * 0.1
            when cat_eligibility.collection_id is not null
                then public.sigmoid(risk_similarity, 3) * 0.6 / 0.7 +
                     public.sigmoid(category_similarity, 3) * 0.1 / 0.7
            when int_eligibility.collection_id is not null
                then public.sigmoid(risk_similarity, 3) * 0.6 / 0.9 +
                     public.sigmoid(interest_similarity, 3) * 0.3 / 0.9
            else public.sigmoid(risk_similarity, 3)
            end * 100)::int                                                as match_score,
       risk_similarity,
       category_similarity,
       interest_similarity,
       now()                                                               as updated_at,
       (risk_similarity > 0.3)::int + (risk_similarity > 0.7)::int         as risk_level,
       (category_similarity > 0.3)::int + (category_similarity > 0.7)::int as category_level,
       (interest_similarity > 0.3)::int + (interest_similarity > 0.7)::int as interest_level
from risk_similarity
         left join cat_eligibility using (collection_id)
         left join int_eligibility using (collection_id)
         left join cat_int_similarity using (profile_id, collection_uniq_id)
on conflict (
    profile_id, collection_uniq_id
    ) do update set profile_id          = excluded.profile_id,
                    collection_id       = excluded.collection_id,
                    collection_uniq_id  = excluded.collection_uniq_id,
                    match_score         = excluded.match_score,
                    risk_similarity     = excluded.risk_similarity,
                    category_similarity = excluded.category_similarity,
                    interest_similarity = excluded.interest_similarity,
                    updated_at          = excluded.updated_at,
                    risk_level          = excluded.risk_level,
                    category_level      = excluded.category_level,
                    interest_level      = excluded.interest_level;
