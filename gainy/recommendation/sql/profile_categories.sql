select profile_id as id, json_object_agg(category_id, 1.0) as profile_category_vector
from app.profile_categories
where profile_id = %(profile_id)s
group by profile_id;
