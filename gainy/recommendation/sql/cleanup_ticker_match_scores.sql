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
                      join app.profile_ticker_match_score using (profile_id)
             group by profile_id
         )
delete
from app.profile_ticker_match_score
    using last_version
where profile_ticker_match_score.profile_id = last_version.profile_id
  and profile_ticker_match_score.updated_at < last_version.updated_at;

with profiles as
         (
             select id as profile_id, email
             from app.profiles
             {where_clause}
         )
delete
from app.profile_ticker_match_score
    using profiles
where profile_ticker_match_score.profile_id = profiles.profile_id
 and (profile_ticker_match_score.profile_id not in (select profile_id from app.profile_scoring_settings)
  or symbol not in (select symbol from tickers where ms_enabled));