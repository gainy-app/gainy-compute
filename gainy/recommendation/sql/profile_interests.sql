with profile_industries as
         (
             select distinct profile_id, interest_id, industry_id
             from app.profile_interests
--                       join ticker_interests using (interest_id)
--                       join ticker_industries using (symbol)
                      join interest_industries using (interest_id)
             where profile_id = %(profile_id)s
         ),
     profile_industries_counts as
         (
             select interest_id, industry_id, count(*) as industry_count
             from profile_industries
             group by interest_id, industry_id
         )
select interest_id, json_object_agg(industry_id, industry_count) as profile_interest_vector
from profile_industries_counts
group by interest_id
