
-- Full name: Hoang Nhat Tam Dang

-- In this project, we will write 08 query in Bigquery base on Google Analytics dataset.


----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 1 ** Calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
----------------------------------------------------------------------------------------------------------------------------------------------
select
  concat(extract(year from parse_date('%Y%m%d', date)), format("%02d", extract(month from parse_date('%Y%m%d', date)))) month
  , sum(totals.visits) visits
  , sum(totals.pageviews) pageviews
  , sum(totals.transactions) transactions
from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
where _table_suffix between '0101' and '0331'
group by month
order by month;


----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 2 ** Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
----------------------------------------------------------------------------------------------------------------------------------------------
select  
  trafficSource.source source
  , sum(totals.visits) total_visits
  , sum(totals.bounces) total_no_of_bounces
  ,(sum(totals.Bounces)/sum(totals.visits))* 100 as bounce_rate
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by source
order by total_visits desc;


----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 3 ** Revenue by traffic source by week, by month in June 2017
----------------------------------------------------------------------------------------------------------------------------------------------
-- CTE of separate month and week
  with month_rev as (
                  select  
                    'Month' time_type
                    ,format_date("%Y%m", parse_date("%Y%m%d", date)) as month
                    , trafficSource.source source
                    , round(sum(productRevenue/1000000.0), 4) revenue 
                  from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
                    unnest(hits) hits
                  , unnest(hits.product)
                  where productRevenue is not null
                  group by source, time
                  order by revenue desc)

  , week_rev as (
                  select  
                    'Week' time_type
                    ,format_date("%Y%W", parse_date("%Y%m%d", date)) as week
                    , trafficSource.source source
                    , round(sum(productRevenue/1000000.0), 4) revenue 
                  from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
                    unnest(hits) hits
                  , unnest(hits.product)
                  where productRevenue is not null
                  group by source, time
                  order by revenue desc
  )

-- Select the final output
  select 
    *
  from month_rev
  union all
  select *
  from week_rev
  order by revenue desc;



----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 4 ** Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017
----------------------------------------------------------------------------------------------------------------------------------------------
-- CTE purchaser and non-purchaser
  with pur as (
            select
              --concat(extract(year from parse_date('%Y%m%d', date)), format("%02d", extract(month from parse_date('%Y%m%d', date)))) month
              format_date("%Y%m",parse_date("%Y%m%d",date)) as month
              , round((sum(totals.pageviews) / count(distinct fullVisitorId)), 7) avg_pageviews_purchase
            from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
            unnest(hits) hits
            , unnest(hits.product)
            where _table_suffix between '0601' and '0731'
                  and totals.transactions >= 1
                  and productRevenue is not null
            group by month)

  , non_pur as (
            select
              --concat(extract(year from parse_date('%Y%m%d', date)), format("%02d", extract(month from parse_date('%Y%m%d', date)))) month
              format_date("%Y%m",parse_date("%Y%m%d",date)) as month
              , round((sum(totals.pageviews) / count(distinct fullVisitorId)), 7) avg_pageviews_non_purchase
            from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
            unnest(hits) hits
            , unnest(hits.product)
            where _table_suffix between '0601' and '0731'
                  and totals.transactions is null
                  and productRevenue is null
            group by month
  )

-- Select the final output
  select
    *
  from pur
  full join non_pur using(month)  
  order by month;


----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 5 ** Average number of transactions per user that made a purchase in July 2017
----------------------------------------------------------------------------------------------------------------------------------------------
select
  --concat(extract(year from parse_date('%Y%m%d', date)), format("%02d", extract(month from parse_date('%Y%m%d', date)))) Month
  format_date("%Y%m",parse_date("%Y%m%d",date)) as month
  , round((sum(totals.transactions) / count(distinct fullVisitorId)), 9) Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
unnest(hits) hits
, unnest(hits.product)
where totals.transactions >= 1
      and productRevenue is not null
group by month;



----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 6 ** Average amount of money spent per session. Only include purchaser data in July 2017
----------------------------------------------------------------------------------------------------------------------------------------------
select
  --concat(extract(year from parse_date('%Y%m%d', date)), format("%02d", extract(month from parse_date('%Y%m%d', date)))) Month
  format_date("%Y%m",parse_date("%Y%m%d",date)) as month
  , round(((sum(productRevenue)/1000000) / sum(totals.visits)), 2) avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
unnest(hits) hits
, unnest(hits.product)
where totals.transactions >= 1
      and productRevenue is not null
      and totals.transactions is not null
group by month; 



----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 7 ** Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. 
--               Output should show product name and the quantity was ordered.
----------------------------------------------------------------------------------------------------------------------------------------------
-- Step 1: List the users that have already purchased "YouTube Men's Vintage Henley" in July 2017
with buyer as (
              select
                distinct fullVisitorId
              from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
              unnest(hits) hits
              , unnest(hits.product) as product
              where v2ProductName = "YouTube Men's Vintage Henley"
                    and productRevenue is not null)

-- Step 2: Select the rest of productname for such users
select
  v2ProductName other_purchased_products
  , sum(productQuantity) quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
unnest(hits) hits
, unnest(hits.product) as product
inner join buyer using (fullVisitorId)
where v2ProductName <> "YouTube Men's Vintage Henley"
      and productRevenue is not null
group by other_purchased_products
order by quantity desc;


----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 8 ** Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. 
--               For example, 100% product view then 40% add_to_cart and 10% purchase. 
--               Add_to_cart_rate = number product  add to cart/number product view. 
--               Purchase_rate = number product purchase/number product view. The output should be calculated in product level.
----------------------------------------------------------------------------------------------------------------------------------------------
-- Create CTEs of 'num_product_view', 'num_addtocart', 'num_purchase'
with view as (
              select
                --concat(extract(year from parse_date('%Y%m%d', date)), format("%02d", extract(month from parse_date('%Y%m%d', date)))) month
                format_date("%Y%m",parse_date("%Y%m%d",date)) as month
                , count(eCommerceAction.action_type) num_product_view
              from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
              unnest(hits) hits
              , unnest(hits.product)
              where _table_suffix between '0101' and '0331'
                    and eCommerceAction.action_type = '2' 
              group by month
              order by month)

, cart as (
          select
            --concat(extract(year from parse_date('%Y%m%d', date)), format("%02d", extract(month from parse_date('%Y%m%d', date)))) month
            format_date("%Y%m",parse_date("%Y%m%d",date)) as month
            , count(eCommerceAction.action_type) num_addtocart
          from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
          unnest(hits) hits
          , unnest(hits.product)
          where _table_suffix between '0101' and '0331'
                and eCommerceAction.action_type = '3' 
          group by month
          order by month)

, purchase as (
              select
                --concat(extract(year from parse_date('%Y%m%d', date)), format("%02d", extract(month from parse_date('%Y%m%d', date)))) month
                format_date("%Y%m",parse_date("%Y%m%d",date)) as month
                , count(eCommerceAction.action_type) num_purchase
              from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
              unnest(hits) hits
              , unnest(hits.product)
              where _table_suffix between '0101' and '0331'
                    and eCommerceAction.action_type = '6'
                    and productRevenue is not null
              group by month
              order by month
)


-- Select final output
select
  *
  , round((100*num_addtocart / num_product_view), 2) add_to_cart_rate
  , round((100*num_purchase / num_product_view), 2) purchase_rate
from view
left join cart using(month)
left join purchase using(month)
order by month;

-----------------------------------------------
-- THE END - THANK YOU VERY MUCH FOR YOUR TIME
-----------------------------------------------


