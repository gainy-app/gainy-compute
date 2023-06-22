insert into app.object_recommendation_state (object_id, object_type, state_hash)
select symbol, 'ticker', state_hash
from ticker_actual_recommendation_state
on conflict (object_id, object_type) do update set state_hash = excluded.state_hash,
                                                   updated_at = now();
