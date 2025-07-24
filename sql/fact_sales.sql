WITH product_price AS (
  SELECT product_id, unit_price
  FROM `purwadika.jcdeol3_final_project_riki.stg_product`
),
sales_cleaned AS (
  SELECT
    s.transaction_id,
    s.transaction_date,
    s.store_id,
    s.product_id,
    p.unit_price AS unit_price,
    s.transaction_qty,
    p.unit_price * s.transaction_qty AS revenue
  FROM
    `purwadika.jcdeol3_final_project_riki.stg_sales` s
  JOIN product_price p
    ON s.product_id = p.product_id
  WHERE
    {% if params.full_load %}
      TRUE
    {% else %}
      DATE(s.transaction_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    {% endif %}
    AND s.transaction_id IS NOT NULL
    AND s.transaction_date IS NOT NULL
    AND s.store_id IS NOT NULL
    AND s.product_id IS NOT NULL
    AND s.transaction_qty IS NOT NULL
)
SELECT * FROM sales_cleaned