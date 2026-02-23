--------------------------------------------------
-- Database & Schema
--------------------------------------------------

CREATE DATABASE IF NOT EXISTS ECOMMERCE_DW;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DW.ANALYTICS;

USE DATABASE ECOMMERCE_DW;
USE SCHEMA ANALYTICS;

--------------------------------------------------
-- Dimension Tables
--------------------------------------------------

CREATE OR REPLACE TABLE DIM_CUSTOMER
(
    CUSTOMER_KEY INTEGER AUTOINCREMENT,
    CUSTOMER_ID  INTEGER,
    name         STRING,
    EMAIL        STRING,
    CITY         STRING,
    COUNTRY      STRING
);

CREATE OR REPLACE TABLE DIM_PRODUCT
(
    PRODUCT_KEY  INTEGER AUTOINCREMENT,
    PRODUCT_ID   INTEGER,
    PRODUCT_NAME STRING,
    CATEGORY     STRING,
    PRICE        NUMBER(10,2)
);

CREATE OR REPLACE TABLE DIM_DATE
(
    DATE_KEY  INTEGER AUTOINCREMENT,
    FULL_DATE DATE
);

--------------------------------------------------
-- Fact Table
--------------------------------------------------

CREATE OR REPLACE TABLE FACT_SALES
(
    CUSTOMER_KEY INTEGER,
    PRODUCT_KEY  INTEGER,
    DATE_KEY     INTEGER,
    ORDER_ID     INTEGER,
    QUANTITY     INTEGER
);

--------------------------------------------------
-- Staging Tables
--------------------------------------------------

CREATE OR REPLACE TABLE ORDERS_TEMP
(
    ORDER_ID     INTEGER,
    CUSTOMER_ID  INTEGER,
    ORDER_DATE   DATE
);

CREATE OR REPLACE TABLE ORDER_ITEMS_TEMP
(
    ORDER_ITEM_ID INTEGER,
    ORDER_ID      INTEGER,
    PRODUCT_ID    INTEGER,
    QUANTITY      INTEGER
);

--------------------------------------------------
-- Final check
--------------------------------------------------

SHOW TABLES IN SCHEMA ECOMMERCE_DW.ANALYTICS;

--------------------------------------------------
-- Data validation
--------------------------------------------------

SELECT COUNT(*) FROM DIM_CUSTOMER;
SELECT COUNT(*) FROM DIM_PRODUCT;
SELECT COUNT(*) FROM DIM_DATE;
SELECT COUNT(*) FROM FACT_SALES;

--------------------------------------------------
-- Analytics query (CTE + window function)
--------------------------------------------------

WITH customer_sales AS (
    SELECT
        c.customer_id,
        c.name,
        SUM(f.quantity) AS total_items
    FROM fact_sales f
    JOIN dim_customer c
        ON f.customer_key = c.customer_key
    GROUP BY c.customer_id, c.name
)
SELECT *,
       RANK() OVER (ORDER BY total_items DESC) AS customer_rank
FROM customer_sales;

--------------------------------------------------
-- Recommendation query
--------------------------------------------------

SELECT
    p1.product_name   AS base_product,
    p2.product_name   AS recommended_product,
    COUNT(*)          AS times_bought_together
FROM fact_sales f1
JOIN fact_sales f2
     ON f1.order_id = f2.order_id
    AND f1.product_key <> f2.product_key
JOIN dim_product p1
     ON f1.product_key = p1.product_key
JOIN dim_product p2
     ON f2.product_key = p2.product_key
GROUP BY p1.product_name, p2.product_name
ORDER BY times_bought_together DESC;

--------------------------------------------------
-- Performance optimization
--------------------------------------------------

ALTER TABLE FACT_SALES
CLUSTER BY (DATE_KEY);

SHOW TABLES IN SCHEMA ECOMMERCE_DW.ANALYTICS;