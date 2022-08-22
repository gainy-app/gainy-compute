select c.id as collection_id, coalesce(profile_collection_match_score.match_score, 0.0) as ranking_score
from profile_collections c
         left join app.profile_collection_match_score
                   on c.uniq_id = profile_collection_match_score.collection_uniq_id
where profile_collection_match_score.profile_id = %(profile_id)s
  and c.enabled = '1'
  and c.personalized = '0'
order by profile_collection_match_score.interest_level desc, profile_collection_match_score.match_score desc nulls last
limit %(limit)s;
