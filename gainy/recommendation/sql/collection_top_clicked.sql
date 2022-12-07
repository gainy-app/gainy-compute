with collection_stats as
         (
             select distinct on (collection_id) collection_id, rank
             from top_global_collections
                      join collections on collections.id = top_global_collections.collection_id
             where collections.enabled = '1'
               and collections.personalized = '0'
         )
select *
from collection_stats
order by rank
limit %(limit)s;
