with collection_stats as
         (
             select collection_id, count(*) as cnt
             from app.profile_favorite_collections
                      join collections on collections.id = profile_favorite_collections.collection_id
             where collections.enabled = '1'
               and collections.personalized = '0'
             group by collection_id
         )
select *
from collection_stats
order by cnt desc
limit %(limit)s;
