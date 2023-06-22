select symbol
from ticker_actual_recommendation_state
         left join app.object_recommendation_state
                   on object_recommendation_state.object_id = symbol
                       and object_recommendation_state.object_type = 'ticker'
where ticker_actual_recommendation_state.state_hash != object_recommendation_state.state_hash
   or object_recommendation_state.state_hash is null