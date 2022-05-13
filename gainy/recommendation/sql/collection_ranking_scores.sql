select c.id as collection_id, coalesce(collection_match_score.match_score, 0.0) as ranking_score
from profile_collections c
         left join collection_match_score
                   on c.id = collection_match_score.collection_id
where collection_match_score.profile_id = %(profile_id)s
  and c.enabled = '1'
  and c.personalized = '0'
order by ranking_score desc
limit %(limit)s;
