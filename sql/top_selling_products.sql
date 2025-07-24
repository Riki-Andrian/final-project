SELECT
    p.product_detail as product,
    SUM(sa.transaction_qty) AS total_quantity_sold
FROM
    `purwadika.jcdeol3_final_project_riki.fact_sales` sa
    JOIN `purwadika.jcdeol3_final_project_riki.dim_product` p ON sa.product_id = p.product_id
GROUP BY
    product
ORDER BY
    total_quantity_sold DESC
LIMIT
    5