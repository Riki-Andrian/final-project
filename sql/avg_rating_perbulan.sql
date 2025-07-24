SELECT
    s.store_location as store,
    FORMAT_DATE('%Y-%m', s.created_at) AS year_month,
    ROUND(AVG(rating), 2) AS avg_rating
FROM
     `purwadika.jcdeol3_final_project_riki.dim_rating` r
JOIN `purwadika.jcdeol3_final_project_riki.dim_store` s ON r.store_id = s.store_id
GROUP BY
    store,
    year_month
ORDER BY
    year_month;
