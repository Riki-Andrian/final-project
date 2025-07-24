SELECT
  product_id,
  product_category,
  product_type,
  product_detail,
  unit_price,
  created_at
FROM `purwadika.jcdeol3_final_project_riki.stg_product`
WHERE
  {% if params.full_load %}
    TRUE
  {% else %}
    DATE(created_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  {% endif %}
  AND product_category IS NOT NULL
  AND product_type IS NOT NULL
  AND product_detail IS NOT NULL
  AND unit_price IS NOT NULL

