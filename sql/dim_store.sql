SELECT
  store_id,
  store_location,
  created_at
FROM `purwadika.jcdeol3_final_project_riki.stg_store`
WHERE
  {% if params.full_load %}
    TRUE
  {% else %}
    DATE(created_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  {% endif %}
  AND store_location IS NOT NULL

