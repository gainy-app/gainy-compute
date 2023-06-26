select profile_id
from profile_actual_recommendation_state
         left join app.object_recommendation_state
                   on object_recommendation_state.object_id = profile_id::text
                       and object_recommendation_state.object_type = 'profile'
         join app.profiles
              on profiles.id = profile_id
where email not ilike '%test%@gainy.app'
  and (profile_actual_recommendation_state.state_hash != object_recommendation_state.state_hash
    or object_recommendation_state.state_hash is null)
