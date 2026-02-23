USE DATABASE ECOMMERCE_DW;
USE SCHEMA ANALYTICS;

-- Check tables
SHOW TABLES IN SCHEMA ECOMMERCE_DW.ANALYTICS;

-- Data validation
SELECT COUNT(*) FROM ECOMMERCE_DW.ANALYTICS.DIM_CUSTOMER;
SELECT COUNT(*) FROM ECOMMERCE_DW.ANALYTICS.DIM_PRODUCT;
SELECT COUNT(*) FROM ECOMMERCE_DW.ANALYTICS.DIM_DATE;
SELECT COUNT(*) FROM ECOMMERCE_DW.ANALYTICS.FACT_SALES;

-- CTE + window function (customer ranking)
WITH customer_sales AS (
    SELECT
        c.customer_id,
        c.name,
        SUM(f.quantity) AS total_items
    FROM ECOMMERCE_DW.ANALYTICS.FACT_SALES f
    JOIN ECOMMERCE_DW.ANALYTICS.DIM_CUSTOMER c
        ON f.customer_key = c.customer_key
    GROUP BY c.customer_id, c.name
)
SELECT *,
       RANK() OVER (ORDER BY total_items DESC) AS customer_rank
FROM customer_sales;

-- Recommendation query (chart 2 query)
SELECT
    p1.product_name   AS base_product,
    p2.product_name   AS recommended_product,
    COUNT(*)          AS times_bought_together
FROM ECOMMERCE_DW.ANALYTICS.FACT_SALES f1
JOIN ECOMMERCE_DW.ANALYTICS.FACT_SALES f2
     ON f1.order_id = f2.order_id
    AND f1.product_key <> f2.product_key
JOIN ECOMMERCE_DW.ANALYTICS.DIM_PRODUCT p1
     ON f1.product_key = p1.product_key
JOIN ECOMMERCE_DW.ANALYTICS.DIM_PRODUCT p2
     ON f2.product_key = p2.product_key
GROUP BY p1.product_name, p2.product_name
ORDER BY times_bought_together DESC;

-- Performance optimization
ALTER TABLE ECOMMERCE_DW.ANALYTICS.FACT_SALES
CLUSTER BY (DATE_KEY);

SHOW WAREHOUSES;