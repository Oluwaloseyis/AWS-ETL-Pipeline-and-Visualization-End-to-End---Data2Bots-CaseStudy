/*
    This model handles question 2 and 3 of the problem.md document
    i.e. Total number of late shipments and the Total number of undelivered shipments 

*/

{{ config(materialized='table') }}

select CURRENT_DATE ingestion_date, 
(select 
COUNT(*)
FROM oluwseko5931_staging.orders a
,oluwseko5931_staging.shipment_deliveries b
where a.order_id = b.order_id 
and to_date(b.shipment_date, 'YYYY-MM-DD') >= to_date(a.order_date, 'YYYY-MM-DD') +6
and delivery_date is NULL)tt_late_shipments,
(select 
COUNT(*) 
FROM oluwseko5931_staging.orders a
, oluwseko5931_staging.shipment_deliveries b
where a.order_id = b.order_id 
and to_date('2022-09-01', 'YYYY-MM-DD') >= to_date(a.order_date, 'YYYY-MM-DD') +15
and delivery_date is NULL and b.shipment_date is NULL) tt_undelivered_items
