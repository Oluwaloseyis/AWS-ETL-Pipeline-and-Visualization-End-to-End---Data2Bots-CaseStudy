/*
    This model handles question 2 and 3 of the problem.md document
    i.e. Total number of late shipments and the Total number of undelivered shipments 

*/

{{ config(materialized='table') }}

select CURRENT_DATE ingestion_date,
(select product_name from if_common.dim_products a where product_id =
(select product_id from (select product_id, count(*) FROM oluwseko5931_staging.reviews group by product_id order by 2 desc
LIMIT 1) foo)::INTEGER) product_name,
(select order_date from 
(select to_date(a.order_date, 'YYYY-MM-DD') order_date, count(*) count FROM oluwseko5931_staging.orders a where product_id =
(select product_id from (select product_id, count(*) FROM oluwseko5931_staging.reviews group by product_id order by 2 desc
LIMIT 1) foo) group by to_date(a.order_date, 'YYYY-MM-DD') order by 2 desc LIMIT 1)foo) most_ordered_day,
(select CASE WHEN day_of_the_week_num between 1 and 5 and working_day is false THEN true else false end as is_public_holiday 
from if_common.dim_dates where calendar_dt = (select order_date from 
(select to_date(a.order_date, 'YYYY-MM-DD') order_date, count(*) count FROM oluwseko5931_staging.orders a where product_id =
(select product_id from (select product_id, count(*) FROM oluwseko5931_staging.reviews group by product_id order by 2 desc
LIMIT 1) foo) group by to_date(a.order_date, 'YYYY-MM-DD') order by 2 desc LIMIT 1)foo)) is_public_holiday,
(select sum from (select product_id, count(*) COUNT, SUM(review::INTEGER) SUM FROM oluwseko5931_staging.reviews group by product_id order by 2 desc
LIMIT 1) foo) tt_review_points,
(select pct_one_star_review from( 
select product_id, count(*) count, 
(sum(case when review = '1' then 1 else 0 end)::decimal /count(*)::decimal )*100 pct_one_star_review
FROM oluwseko5931_staging.reviews group by product_id order by 2 desc
LIMIT 1) foo) pct_one_star_review,
(select pct_two_star_review from( 
select product_id, count(*) count, 
(sum(case when review = '2' then 1 else 0 end)::decimal /count(*)::decimal )*100 pct_two_star_review
FROM oluwseko5931_staging.reviews group by product_id order by 2 desc
LIMIT 1) foo) pct_two_star_review,
(select pct_three_star_review from( 
select product_id, count(*) count, 
(sum(case when review = '3' then 1 else 0 end)::decimal /count(*)::decimal )*100 pct_three_star_review
FROM oluwseko5931_staging.reviews group by product_id order by 2 desc
LIMIT 1) foo) pct_three_star_review,
(select pct_four_star_review from( 
select product_id, count(*) count, 
(sum(case when review = '4' then 1 else 0 end)::decimal /count(*)::decimal )*100 pct_four_star_review
FROM oluwseko5931_staging.reviews group by product_id order by 2 desc
LIMIT 1) foo) pct_four_star_review,
(select pct_five_star_review from( 
select product_id, count(*) count, 
(sum(case when review = '3' then 1 else 0 end)::decimal /count(*)::decimal )*100 pct_five_star_review
FROM oluwseko5931_staging.reviews group by product_id order by 2 desc
LIMIT 1) foo) pct_five_star_review,
(SELECT ((select COUNT(*)FROM oluwseko5931_staging.orders a, oluwseko5931_staging.shipment_deliveries b
where product_id =(select product_id from (select product_id, count(*) FROM oluwseko5931_staging.reviews group by product_id order by 2 desc
LIMIT 1) foo)  and a.order_id = b.order_id and to_date(b.shipment_date, 'YYYY-MM-DD') < to_date(a.order_date, 'YYYY-MM-DD') +6
and delivery_date is NOT NULL)::decimal/(select COUNT(*)FROM oluwseko5931_staging.orders where product_id = (select product_id from (select product_id, count(*) FROM oluwseko5931_staging.reviews group by product_id order by 2 desc
LIMIT 1) foo))::decimal) * 100) pct_early_shipments,
(SELECT ((select COUNT(*)FROM oluwseko5931_staging.orders a, oluwseko5931_staging.shipment_deliveries b
where product_id =(select product_id from (select product_id, count(*) FROM oluwseko5931_staging.reviews group by product_id order by 2 desc
LIMIT 1) foo)  and a.order_id = b.order_id and to_date(b.shipment_date, 'YYYY-MM-DD') >= to_date(a.order_date, 'YYYY-MM-DD') +6
and delivery_date is NULL)::decimal/(select COUNT(*)FROM oluwseko5931_staging.orders where product_id = (select product_id from (select product_id, count(*) FROM oluwseko5931_staging.reviews group by product_id order by 2 desc
LIMIT 1) foo))::decimal) * 100) pct_late_shipments