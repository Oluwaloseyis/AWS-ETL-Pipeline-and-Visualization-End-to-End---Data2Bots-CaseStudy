/*
    This model handles question 1 of the problem.md document
    i.e. The total number of orders placed on a public holiday every month for the past year
    The past year is 2022

*/

{{ config(materialized='table') }}

SELECT CURRENT_DATE ingestion_date ,
SUM(CASE WHEN TO_CHAR(to_date(a.order_date, 'YYYY-MM-DD'), 'Mon') = 'Jan' THEN 1 else 0 end) tt_order_hol_jan,
SUM(CASE WHEN TO_CHAR(to_date(a.order_date, 'YYYY-MM-DD'), 'Mon') = 'Feb' THEN 1 else 0 end) tt_order_hol_feb,
SUM(CASE WHEN TO_CHAR(to_date(a.order_date, 'YYYY-MM-DD'), 'Mon') = 'Mar' THEN 1 else 0 end) tt_order_hol_mar,
SUM(CASE WHEN TO_CHAR(to_date(a.order_date, 'YYYY-MM-DD'), 'Mon') = 'Apr' THEN 1 else 0 end) tt_order_hol_apr,
SUM(CASE WHEN TO_CHAR(to_date(a.order_date, 'YYYY-MM-DD'), 'Mon') = 'May' THEN 1 else 0 end) tt_order_hol_may,
SUM(CASE WHEN TO_CHAR(to_date(a.order_date, 'YYYY-MM-DD'), 'Mon') = 'Jun' THEN 1 else 0 end) tt_order_hol_jun,
SUM(CASE WHEN TO_CHAR(to_date(a.order_date, 'YYYY-MM-DD'), 'Mon') = 'Jul' THEN 1 else 0 end) tt_order_hol_jul,
SUM(CASE WHEN TO_CHAR(to_date(a.order_date, 'YYYY-MM-DD'), 'Mon') = 'Aug' THEN 1 else 0 end) tt_order_hol_aug,
SUM(CASE WHEN TO_CHAR(to_date(a.order_date, 'YYYY-MM-DD'), 'Mon') = 'Sep' THEN 1 else 0 end) tt_order_hol_sep,
SUM(CASE WHEN TO_CHAR(to_date(a.order_date, 'YYYY-MM-DD'), 'Mon') = 'Oct' THEN 1 else 0 end) tt_order_hol_oct,
SUM(CASE WHEN TO_CHAR(to_date(a.order_date, 'YYYY-MM-DD'), 'Mon') = 'Nov' THEN 1 else 0 end) tt_order_hol_nov,
SUM(CASE WHEN TO_CHAR(to_date(a.order_date, 'YYYY-MM-DD'), 'Mon') = 'Dec' THEN 1 else 0 end) tt_order_hol_dec
FROM oluwseko5931_staging.orders a, if_common.dim_dates b
where to_date(a.order_date, 'YYYY-MM-DD') = b.calendar_dt 
and DATE_PART('YEAR', to_date(a.order_date, 'YYYY-MM-DD')) = 2022
and day_of_the_week_num between 1 and 5 and working_day is false
GROUP BY CURRENT_DATE