SELECT
    st.store_location AS store,
    FORMAT_DATE('%Y-%m', s.transaction_date) AS year_month,
    ROUND(SUM(s.revenue), 2) AS total_revenue
FROM
    `purwadika.jcdeol3_final_project_riki.fact_sales` s
JOIN
    `purwadika.jcdeol3_final_project_riki.dim_store` st ON s.store_id = st.store_id
GROUP BY
    st.store_location,
    year_month
ORDER BY
    year_month,
    st.store_location;
