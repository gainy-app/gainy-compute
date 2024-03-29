with profiles as
         (
             select id as profile_id, email
             from app.profiles
             {where_clause}
         ),
     last_version as
         (
             select profile_id, max(updated_at) as updated_at
             from profiles
                      join app.profile_collection_match_score using (profile_id)
             group by profile_id
         )
delete
from app.profile_collection_match_score
    using last_version
where profile_collection_match_score.profile_id = last_version.profile_id
  and profile_collection_match_score.updated_at < last_version.updated_at;
