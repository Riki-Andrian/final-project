SELECT
  store_id,
  first_name,
  rating,
  created_at
FROM `purwadika.jcdeol3_final_project_riki.stg_rating`
WHERE
  {% if params.full_load %}
    TRUE
  {% else %}
    DATE(created_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  {% endif %}
  AND first_name IS NOT NULL
  AND rating IS NOT NULL
