select profile_collections.id
from collection_metrics
         join profile_collections on profile_collections.uniq_id = collection_uniq_id
where profile_collections.enabled = '1'
  and profile_collections.personalized = '0'
order by value_change_1m desc
limit %(limit)s;
