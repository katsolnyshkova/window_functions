select *
from superstore_dataset
limit 10

	
-- 1. let's rank categories by profit (DENSE_RANK)
with t1 as (
SELECT 
	segment, 
	subcategory, 
	round(sum(profit)) as profit_sum
FROM superstore_dataset
GROUP BY segment, subcategory )
	
SELECT 
	dense_rank() over w as rank,
	segment, 
	subcategory, 
	profit_sum

FROM t1
window w as (order by profit_sum desc)
ORDER BY profit_sum desc



-- 2. let's rank categories by profit by each segment (DENSE_RANK with PARTITION BY)
with t1 as (
SELECT 
	segment, 
	subcategory, 
	round(sum(profit)) as profit_sum
FROM superstore_dataset
GROUP BY segment, subcategory )
	
SELECT 
	dense_rank() over w as rank,
	segment, 
	subcategory, 
	profit_sum

FROM t1
window w as (partition by segment order by profit_sum desc)
ORDER BY segment, profit_sum desc


-- 3. let's devide all subcategories into 3 groups (NTILE)
with t1 as (
SELECT 
	segment, 
	subcategory, 
	round(sum(profit)) as profit_sum
FROM superstore_dataset
GROUP BY segment, subcategory )
	
SELECT 
	ntile(3) over w as rank,
	segment, 
	subcategory, 
	profit_sum

FROM t1
window w as (order by profit_sum desc)
ORDER BY profit_sum desc


-- 4. let's devide all subcategories into 3 groups by each segment (NTILE with PARTITION BY)
with t1 as (
SELECT 
	segment, 
	subcategory, 
	round(sum(profit)) as profit_sum
FROM superstore_dataset
GROUP BY segment, subcategory )
	
SELECT 
	ntile(3) over w as rank,
	segment, 
	subcategory, 
	profit_sum

FROM t1
window w as (partition by segment order by profit_sum desc)
ORDER BY segment, profit_sum desc


-- 5. let's only see the most profitable subcategories within each segment (NTILE with PARTITION BY)
with t2 as (
with t1 as (
SELECT 
	segment, 
	subcategory, 
	round(sum(profit)) as profit_sum
FROM superstore_dataset
GROUP BY segment, subcategory )
	
SELECT 
	ntile(3) over w as rank,
	segment, 
	subcategory, 
	profit_sum

FROM t1
window w as (partition by segment order by profit_sum desc)
ORDER BY segment, profit_sum desc)

SELECT *
FROM t2
WHERE rank = 1


-- 6. let's compare subcategory's profit with next one (LEAD)
with t1 as (
SELECT 
	segment, 
	subcategory, 
	round(sum(profit)) as profit_sum
FROM superstore_dataset
GROUP BY segment, subcategory )
	
SELECT 
	dense_rank() over w as rank,
	segment, 
	subcategory, 
	profit_sum,
	lead(profit_sum) over w as prev,
	round((profit_sum - (lead(profit_sum) over w))*100/lead(profit_sum) over w) as diff
	
FROM t1
window w as (order by profit_sum desc)
ORDER BY profit_sum desc


-- 7. let's compare subcategory's profit with MAX and MIN (FISRT_VALUE and LAST_VALUE)
with t1 as (
SELECT 
	segment, 
	subcategory, 
	round(sum(profit)) as profit_sum
FROM superstore_dataset
GROUP BY segment, subcategory )
	
SELECT 
	dense_rank() over w as rank,
	segment, 
	subcategory, 
	profit_sum,
	first_value(profit_sum) over w as max_profit,
	last_value(profit_sum) over w as min_profit
	
FROM t1
window w as (partition by segment order by profit_sum desc rows between unbounded preceding and unbounded following)
ORDER BY segment, profit_sum desc


-- 8. let's check subcategory's percentage of the subcategory with max profit in segment (FIRST_VALUE)
with t1 as (
SELECT 
	segment, 
	subcategory, 
	round(sum(profit)) as profit_sum
FROM superstore_dataset
GROUP BY segment, subcategory )
	
SELECT 
	dense_rank() over w as rank,
	segment, 
	subcategory, 
	profit_sum,
	first_value(profit_sum) over w as max_profit,
	round(profit_sum*100 / first_value(profit_sum) over w) as perc
	
FROM t1
window w as (partition by segment order by profit_sum desc rows between unbounded preceding and unbounded following)
ORDER BY segment, profit_sum desc


-- 9. let's calculate total profit by segment and see subcategory's share (SUM)
with t1 as (
SELECT 
	segment, 
	subcategory, 
	round(sum(profit)) as profit_sum
FROM superstore_dataset
GROUP BY segment, subcategory )
	
SELECT 
	dense_rank() over w as rank,
	segment, 
	subcategory, 
	profit_sum,
	sum(profit_sum) over w as total_profit,
	round((profit_sum*100)/sum(profit_sum) over w) as perc
	
FROM t1
window w as (partition by segment )
ORDER BY segment, profit_sum desc


-- 10. let's see at the avg profit of previous, current and following month (AVG, frames)
with t1 as (
SELECT
	order_year, 
	order_month,
	round(SUM(profit)) as profit
FROM superstore_dataset
GROUP BY order_year, order_month
ORDER BY order_year, order_month)

SELECT 
	order_year, 
	order_month,
	profit,
	round(avg(profit) over w) as roll_avg
FROM t1
window w as (order by order_year, order_month rows between 1 preceding and 1 following)
ORDER BY order_year, order_month



	
-- 11. let's see accumulated sales and profit (SUM, frames)
with t1 as (
SELECT
	order_year, 
	order_month,
	round(SUM(sales)) as sales,
	round(SUM(profit)) as profit
FROM superstore_dataset
GROUP BY order_year, order_month
ORDER BY order_year, order_month)

SELECT 
	order_year, 
	order_month,
	sales,
	sum(sales) over w as t_sales,
	profit,
	sum(profit) over w as t_profit
FROM t1
window w as (partition by order_year order by order_year, order_month rows between unbounded preceding and current row)
ORDER BY order_year, order_month















