SELECT
    s.store_location as store,
    ROUND(AVG(r.rating),2) AS average_rating
FROM
    `purwadika.jcdeol3_final_project_riki.dim_rating` r
    JOIN `purwadika.jcdeol3_final_project_riki.dim_store` s ON r.store_id = s.store_id
GROUP BY
    store
ORDER BY
    average_rating DESC