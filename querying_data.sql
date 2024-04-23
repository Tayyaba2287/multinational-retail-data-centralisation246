--The Operations team would like to know which countries we currently operate in and which country now has the most stores.

SELECT country_code,
	COUNT(*)
FROM 
	dim_store_details 
GROUP BY
	country_code;
	
--The business stakeholders would like to know which locations currently have the most stores.

SELECT locality, 
	COUNT(*) AS store_count
FROM dim_store_details
GROUP BY locality
HAVING COUNT(*) >= 10
ORDER BY store_count DESC;

-- Query the database to find out which months have produced the most sales.
SELECT month, 
	COUNT(*) AS sales
FROM dim_date_times
GROUP BY month
ORDER BY sales DESC;

-- The company is looking to increase its online sales. They want to know how many sales are happening online vs offline.

SELECT * FROM dim_store_details;

SELECT 
    COUNT(*) FILTER (WHERE store_code LIKE 'WEB%') AS online,
	SUM(product_quantity) FILTER (WHERE store_code LIKE 'WEB%') AS online_quantity,
    COUNT(*) FILTER (WHERE store_code NOT LIKE 'WEB%') AS offline,
	SUM(product_quantity) FILTER (WHERE store_code NOT LIKE 'WEB%') AS offline_quantity
FROM 
    orders_table;
	
--The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.
--Find out the total and percentage of sales coming from each of the different store types.

SELECT 
    store_type,
    total_sales,
    (total_sales / SUM(total_sales) OVER ()) * 100 AS sales_percentage
FROM (
	SELECT 
    sd.store_type,
    SUM(q.product_quantity * p.product_price) AS total_sales
	FROM 
    	orders_table q
	JOIN 
    	dim_products p ON q.product_code = p.product_code
	JOIN 
    	dim_store_details sd ON q.store_code = sd.store_code
	GROUP BY 
    	sd.store_type
) AS sales_data
ORDER BY sales_percentage DESC;

--The company stakeholders want assurances that the company has been doing well recently.
--Find which months in which years have had the most sales historically.

WITH MonthlySales AS (
    SELECT 
        year,
        month,
        COUNT(*) AS sales_count
    FROM 
        dim_date_times
    GROUP BY 
        year, month
),
RankedMonths AS (
    SELECT
        year,
        month,
        sales_count,
        RANK() OVER (PARTITION BY year ORDER BY sales_count DESC) as rank
    FROM 
        MonthlySales
)
SELECT 
    year,
    month,
    sales_count
FROM 
    RankedMonths
WHERE 
    rank = 1;


--The operations team would like to know the overall staff numbers in each location around the world. 
--Perform a query to determine the staff numbers in each of the countries the company sells in.
SELECT country_code,
	SUM(staff_numbers) AS total_staff_numbers
FROM 
	dim_store_details 
GROUP BY 
	country_code
ORDER BY 
	total_staff_numbers DESC;
	
--The sales team is looking to expand their territory in Germany. 
--Determine which type of store is generating the most sales in Germany.

SELECT 
    sd.store_type,
    SUM(q.product_quantity * p.product_price) AS total_sales,
	sd.country_code
FROM 
    orders_table q
JOIN 
    dim_products p ON q.product_code = p.product_code
JOIN 
    dim_store_details sd ON q.store_code = sd.store_code
WHERE sd.country_code = 'DE'
GROUP BY 
    sd.store_type, sd.country_code;
	
--Sales would like the get an accurate metric for how quickly the company is making sales.
--Determine the average time taken between each sale grouped by year.
	
WITH TimeDifferences AS (
    SELECT
        year,
        -- Calculate the difference in seconds to the next sale's timestamp
        EXTRACT(EPOCH FROM (LEAD(timestamp) OVER (PARTITION BY year ORDER BY timestamp) - timestamp)) AS time_diff_seconds
    FROM
        dim_date_times
)
, FilteredTimeDifferences AS (
    SELECT
        year,
        time_diff_seconds
    FROM
        TimeDifferences
    WHERE
        time_diff_seconds IS NOT NULL -- Remove the last sale of each year which has no subsequent sale to compare
)
, AveragedDifferences AS (
    SELECT
        year,
        AVG(time_diff_seconds) AS avg_time_diff_seconds
    FROM
        FilteredTimeDifferences
    GROUP BY
        year
)
SELECT
    year,
    FLOOR(avg_time_diff_seconds / 3600) AS "hours",
    FLOOR((avg_time_diff_seconds % 3600) / 60) AS "minutes",
    FLOOR(avg_time_diff_seconds % 60) AS "seconds"
FROM
    AveragedDifferences;