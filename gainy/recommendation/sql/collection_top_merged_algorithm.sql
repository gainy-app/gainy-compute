with ranked_performance as
         (
             select profile_collections.id                      as collection_id,
                    rank() over (order by value_change_1m desc) as rank
             from collection_metrics
                      join profile_collections on profile_collections.uniq_id = collection_uniq_id
             where profile_collections.enabled = '1'
               and profile_collections.personalized = '0'
         ),
     ranked_clicks as
         (
             select distinct on (collection_id) collection_id, rank
             from top_global_collections
                      join collections on collections.id = top_global_collections.collection_id
             where collections.enabled = '1'
               and collections.personalized = '0'
     ),
     ranked_match_score as
         (
             select collection_id,
                    rank() over (order by match_score desc) as rank
             from (
                      select collection_id, avg(match_score) as match_score
                      from app.profile_collection_match_score
                      group by collection_id
                  ) t
                      join collections on collections.id = t.collection_id
             where collections.enabled = '1'
               and collections.personalized = '0'
     ),
     merged_ranked as
         (
             select collection_id,
                    ranked_performance.rank              as performance_rank,
                    ranked_clicks.rank                   as clicks_rank,
                    ranked_match_score.rank              as match_score_rank,
                    coalesce(ranked_performance.rank, 0) +
                    coalesce(ranked_clicks.rank, 0) +
                    coalesce(ranked_match_score.rank, 0) as rank_sum
             from ranked_performance
                      left join ranked_clicks using (collection_id)
                      left join ranked_match_score using (collection_id)
     )
select *
from merged_ranked
order by rank_sum
limit %(limit)s;
