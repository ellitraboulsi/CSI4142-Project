-- Roll up: total sales amount of each product category in each country
SELECT p.category, d.order_year, SUM(f.total_price) AS total_profit
FROM sales_fact_table f
JOIN product_dimension p ON f.product_key = p.product_key
JOIN order_date_dimension d ON f.order_date_key = d.order_date_key
JOIN location_dimension l ON f.location_key = l.location_key
WHERE l.country IN ('United States', 'Canada')
GROUP BY ROLLUP(p.category, d.order_year)
HAVING p.category IS NOT NULL AND d.order_year IS NOT NULL
ORDER BY p.category ASC, d.order_year ASC;


-- Drill Down
-- Step 1: Aggregate sales data by product category
SELECT p.category, SUM(f.total_price) AS total_sales
FROM sales_fact_table f
JOIN product_dimension p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY p.category ASC, total_sales DESC;

-- Step 2: Drill down to get detailed sales data for each sub-category within each category
SELECT p.category, p.sub_category, SUM(f.total_price) AS total_sales
FROM sales_fact_table f
JOIN product_dimension p ON f.product_key = p.product_key
GROUP BY p.category, p.sub_category
ORDER BY p.category ASC, p.sub_category ASC, total_sales DESC;
	
-- Step 3: Drill down to get detailed sales data for each product within each sub-category
SELECT p.category, p.sub_category, p.product_name, SUM(f.total_price) AS total_sales
FROM sales_fact_table f
JOIN product_dimension p ON f.product_key = p.product_key
GROUP BY p.category, p.sub_category, p.product_name
ORDER BY p.category ASC, p.sub_category ASC, p.product_name ASC, total_sales DESC;


-- Slice Query: Find the sales in California for all Office Supplies
SELECT l.state, p.category, SUM(f.total_price) AS total_sales
FROM sales_fact_table f
JOIN location_dimension l ON f.location_key = l.location_key
JOIN product_dimension p ON f.product_key = p.product_key
WHERE l.state IN ('California')
  AND p.category = 'Office Supplies'
GROUP BY l.state, p.category;


-- Dice Query 1: Compare the profit in the United States for Office Supplies versus Technology
SELECT p.category, SUM(f.total_price) AS total_profit
FROM sales_fact_table f
JOIN location_dimension l ON f.location_key = l.location_key
JOIN product_dimension p ON f.product_key = p.product_key
WHERE l.country = 'United States' AND p.category IN ('Office Supplies', 'Technology')
GROUP BY p.category;


-- Dice Query 2: Total sales amount for the product category 'Technology' in the year 2014 for the markets 'EU' and 'Africa'.
SELECT p.category, l.market, d.order_year, SUM(f.total_price) AS total_sales
FROM sales_fact_table f
JOIN order_date_dimension d ON f.order_date_key = d.order_date_key 
JOIN location_dimension l ON f.location_key = l.location_key
JOIN product_dimension p ON f.product_key = p.product_key
WHERE p.category = 'Technology'
  AND d.order_year = 2014
  AND l.market IN ('EU', 'Africa')
GROUP BY p.category, l.market, d.order_year;


-- COMBINED QUERIES

-- 1. Roll up and Slice: Analyze the total sales for each product category and year, but only for the United States and Canada
SELECT p.category, d.order_year, SUM(f.total_price) AS total_sales
FROM sales_fact_table f
JOIN product_dimension p ON f.product_key = p.product_key
JOIN order_date_dimension d ON f.order_date_key = d.order_date_key
JOIN location_dimension l ON f.location_key = l.location_key
WHERE l.country IN ('United States', 'Canada')
GROUP BY ROLLUP(p.category, d.order_year)
HAVING p.category IS NOT NULL AND d.order_year IS NOT NULL
ORDER BY p.category ASC, d.order_year ASC;


-- 2. Compare the sales in France vs Australia for Technology versus Office Supplies in Dec 2011 versus Nov 2011
SELECT l.country, p.category, d.order_month, d.order_year, SUM(f.total_price) AS total_sales
FROM sales_fact_table f
JOIN product_dimension p ON f.product_key = p.product_key
JOIN location_dimension l ON f.location_key = l.location_key
JOIN order_date_dimension d ON f.order_date_key = d.order_date_key
WHERE 
    (
        (p.category = 'Office Supplies' AND d.order_month = 11 AND d.order_year = 2011 AND l.country = 'Australia')
        OR 
        (p.category = 'Office Supplies' AND d.order_month = 12 AND d.order_year = 2011 AND l.country = 'Australia')
        OR
        (p.category = 'Technology' AND d.order_month = 11 AND d.order_year = 2011 AND l.country = 'Australia')
        OR
        (p.category = 'Technology' AND d.order_month = 12 AND d.order_year = 2011 AND l.country = 'Australia')
        OR
        (p.category = 'Office Supplies' AND d.order_month = 11 AND d.order_year = 2011 AND l.country = 'France')
        OR
        (p.category = 'Office Supplies' AND d.order_month = 12 AND d.order_year = 2011 AND l.country = 'France')
        OR 
        (p.category = 'Technology' AND d.order_month = 11 AND d.order_year = 2011 AND l.country = 'France')
        OR
        (p.category = 'Technology' AND d.order_month = 12 AND d.order_year = 2011 AND l.country = 'France')
    )
    AND l.country IN ('Australia', 'France')
GROUP BY p.category, l.country, d.order_year, d.order_month
ORDER BY l.country, p.category, d.order_month ASC;


-- 3. Compare the sales of tables in the United States for February 2013 versus December 2013,
SELECT 
    d.order_month,
    d.order_year,
    SUM(CASE WHEN p.sub_category = 'Tables' THEN f.total_price ELSE 0 END) AS table_sales
FROM sales_fact_table f
JOIN product_dimension p ON f.product_key = p.product_key
JOIN location_dimension l ON f.location_key = l.location_key
JOIN order_date_dimension d ON f.order_date_key = d.order_date_key
WHERE 
    (
        (p.sub_category = 'Tables' AND d.order_month = 2 AND d.order_year = 2013 AND l.country = 'United States')
        OR 
        (p.sub_category = 'Tables' AND d.order_month = 12 AND d.order_year = 2013 AND l.country = 'United States')
    )
GROUP BY 
    l.country, d.order_month, d.order_year
ORDER BY 
    d.order_month ASC, d.order_year;


-- 4. Analyzing sales data for the Office Supplies and Technology categories in Canadian provinces for the year 2014
SELECT l.state AS province, p.category AS category, d.order_year AS year, SUM(f.quantity_sold) AS total_quantity, SUM(f.total_price) AS total_sales
FROM sales_fact_table f
JOIN location_dimension l ON f.location_key = l.location_key
JOIN product_dimension p ON f.product_key = p.product_key
JOIN order_date_dimension d ON f.order_date_key = d.order_date_key
WHERE
    l.country = 'Canada'
    AND d.order_year = 2014
    AND p.category IN ('Technology', 'Office Supplies')
GROUP BY
    GROUPING SETS ((l.state, p.category, d.order_year), (l.state, p.category), (p.category, d.order_year), (l.state, d.order_year), ())
HAVING p.category IS NOT NULL AND l.state IS NOT NULL AND d.order_year IS NOT NULL
ORDER BY total_quantity ASC;


-- Iceberg query: Five countries with the highest sales growth from 2010 to 2011
SELECT 
    l.country AS country,
    (SUM(CASE WHEN d.order_year = 2011 THEN ft.total_price ELSE 0 END) - 
     SUM(CASE WHEN d.order_year = 2010 THEN ft.total_price ELSE 0 END)) AS sales_growth
FROM sales_fact_table f
JOIN location_dimension l ON f.location_key = l.location_key
JOIN order_date_dimension d ON ft.order_date_key = d.order_date_key
WHERE d.order_year IN (2010, 2011)
GROUP BY l.country
ORDER BY sales_growth DESC
LIMIT 5;


-- Windowing: Compare each product's average price with the average price in its category
SELECT 
    p.sub_category, 
    p.product_name,
    ROUND(avg(o.sales_per_item_before_discount), 2) AS item_price_avg,
    ROUND(avg(avg(o.sales_per_item_before_discount)) OVER (PARTITION BY p.sub_category), 2) AS avg_price_sub_category,
    CASE 
        WHEN ROUND(avg(o.sales_per_item_before_discount), 2) > ROUND(avg(avg(o.sales_per_item_before_discount)) OVER (PARTITION BY p.sub_category), 2) THEN 'Above Avg'
        WHEN ROUND(avg(o.sales_per_item_before_discount), 2) < ROUND(avg(avg(o.sales_per_item_before_discount)) OVER (PARTITION BY p.sub_category), 2) THEN 'Below Avg'
        ELSE 'Equal to Avg'
    END AS price_comparison
FROM 
    sales_fact_table f
JOIN product_dimension p ON f.product_key = p.product_key
JOIN order_dimension o ON f.order_key = o.order_key
GROUP BY p.sub_category, p.product_name;

-- Window Clause: Compares the number of orders in European countries in 2013 to that of the previous and next years.
SELECT order_year, country, num_orders, prev_year_orders, next_year_orders
FROM (
    SELECT 
        d.order_year, 
        l.country, 
        COUNT(*) AS num_orders, 
        LAG(COUNT(*)) OVER (ORDER BY d.order_year) AS prev_year_orders, 
        LEAD(COUNT(*)) OVER (ORDER BY d.order_year) AS next_year_orders
    FROM sales_fact_table f
    JOIN order_date_dimension d ON f.order_date_key = d.order_date_key
    JOIN location_dimension l ON f.location_key = l.location_key
    WHERE d.order_year = 2013 AND l.market = 'EU'
    GROUP BY d.order_year, l.country
) AS order_counts
WHERE prev_year_orders IS NOT NULL AND next_year_orders IS NOT NULL;
