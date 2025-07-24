SELECT
    product_category as category,
    ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM
    `purwadika.jcdeol3_final_project_riki.dim_product`
GROUP BY
    category
ORDER BY
    avg_unit_price DESC