SELECT
    s.store_location as store,
    ROUND(SUM(sa.revenue), 2) AS total_revenue
FROM
    `purwadika.jcdeol3_final_project_riki.fact_sales` sa
    JOIN `purwadika.jcdeol3_final_project_riki.dim_store` s ON sa.store_id = s.store_id
GROUP BY
    store
ORDER BY
    total_revenue DESC