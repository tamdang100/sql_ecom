
-- Full name: Hoang Nhat Tam Dang


----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 1 ** Calculate Quantity of items, Sales value & Order quantity by each Subcategory in L12M
----------------------------------------------------------------------------------------------------------------------------------------------

select
  format_timestamp('%b %Y', date(a.ModifiedDate)) period
  , c.Name
  , sum(a.OrderQty) qty_item
  , sum(LineTotal) total_sales
  , count(distinct SalesOrderID) order_cnt
from `adventureworks2019.Sales.SalesOrderDetail` a 
left join `adventureworks2019.Production.Product` b using(ProductID)
left join `adventureworks2019.Production.ProductSubcategory` c on cast(b.ProductSubcategoryID as int) = c.ProductSubcategoryID
where date(a.ModifiedDate) >= (select date_sub(max(date(ModifiedDate)), interval 12 month) from `adventureworks2019.Sales.SalesOrderDetail`)
group by c.Name, period
order by period desc, c.Name;


----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 2 ** Calculate % YoY growth rate by SubCategory & release top 3 cat with highest grow rate. Round results to 2 decimal
----------------------------------------------------------------------------------------------------------------------------------------------

-- CTEs
with 
sale_info as (
  SELECT 
      FORMAT_TIMESTAMP("%Y", a.ModifiedDate) as yr
      , c.Name
      , sum(a.OrderQty) as qty_item

  FROM `adventureworks2019.Sales.SalesOrderDetail` a 
  LEFT JOIN `adventureworks2019.Production.Product` b on a.ProductID = b.ProductID
  LEFT JOIN `adventureworks2019.Production.ProductSubcategory` c on cast(b.ProductSubcategoryID as int) = c.ProductSubcategoryID

  GROUP BY 1,2
  ORDER BY 2 asc , 1 desc
),

sale_diff as (
  select *
  , lead (qty_item) over (partition by Name order by yr desc) as prv_qty
  , round(qty_item / (lead (qty_item) over (partition by Name order by yr desc)) -1,2) as qty_diff
  from sale_info
  order by 5 desc 
),

rk_qty_diff as (
  select *
      ,dense_rank() over( order by qty_diff desc) dk
  from sale_diff
)

-- Select the final output
select distinct Name
      , qty_item
      , prv_qty
      , qty_diff
from rk_qty_diff 
where dk <=3
order by dk ;


----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 3 ** Ranking Top 3 TeritoryID with biggest Order quantity of every year. 
--               If there's TerritoryID with same quantity in a year, do not skip the rank number
----------------------------------------------------------------------------------------------------------------------------------------------

-- CTE ranking table
with 
  territory_order as(
                    select
                    format_timestamp('%Y', date(a.ModifiedDate)) yr
                    , b.TerritoryID
                    , sum(a.OrderQty) order_cnt
                    from `adventureworks2019.Sales.SalesOrderDetail` a 
                    left join `adventureworks2019.Sales.SalesOrderHeader` b using(SalesOrderID)
                    group by yr, b.TerritoryID
                    order by yr)
  , ranking as(
                    select
                      *
                      , (dense_rank() over(partition by yr order by order_cnt desc)) as rk
                    from territory_order
                    )

-- Select the final output
select
  *
from ranking
where rk <= 3
order by yr desc, rk


----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 4 ** Calculate Total Discount Cost belongs to Seasonal Discount for each SubCategory
----------------------------------------------------------------------------------------------------------------------------------------------

select 
    FORMAT_TIMESTAMP("%Y", ModifiedDate) Year
    , Name
    , sum(disc_cost) as total_cost
from (
      select distinct a.*
      , c.Name
      , d.DiscountPct, d.Type
      , a.OrderQty * d.DiscountPct * UnitPrice as disc_cost 
      from `adventureworks2019.Sales.SalesOrderDetail` a
      LEFT JOIN `adventureworks2019.Production.Product` b on a.ProductID = b.ProductID
      LEFT JOIN `adventureworks2019.Production.ProductSubcategory` c on cast(b.ProductSubcategoryID as int) = c.ProductSubcategoryID
      LEFT JOIN `adventureworks2019.Sales.SpecialOffer` d on a.SpecialOfferID = d.SpecialOfferID
      WHERE lower(d.Type) like '%seasonal discount%' 
)
group by 1,2;


----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 5 ** Retention rate of Customer in 2014 with status of Successfully Shipped (Cohort Analysis)
----------------------------------------------------------------------------------------------------------------------------------------------

-- CTEs
with 
		full_month as (
									select
									  extract(month from date(ModifiedDate)) month
									  , extract(year from date(ModifiedDate)) year
									  , CustomerID
									from `adventureworks2019.Sales.SalesOrderHeader`
									where Status = 5
									      and extract(year from date(ModifiedDate)) = 2014
									order by 1, 2)

, row_tab as (
								select
								  *
								  , row_number() over(partition by CustomerID order by month) as row_num
								from full_month)

, first_month as (
								select
								  CustomerID
								  , month month_join
								from row_tab
								where row_num = 1)

, merge_all as (
								select
								  month
								  , month_join
								  ,concat('M - ', month - month_join) month_diff
								  , a.CustomerID
								from full_month a
								left join first_month b using (CustomerID)
								order by a.CustomerID)

-- Select final output
select
  month_join
  , month_diff
  , count(distinct CustomerID) customer_cnt
from merge_all
group by 1, 2
order by 1, 2;


----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 6 ** Trend of Stock level & MoM diff % by all product in 2011. If %gr rate is null then 0. Round to 1 decimal
----------------------------------------------------------------------------------------------------------------------------------------------

-- CTEs
with 
	stock_tab as (
                select
                  Name
                  , extract (month from b.ModifiedDate) mth
                  , extract (year from b.ModifiedDate) yr
                  , sum(StockedQty) stock_qty
                from `adventureworks2019.Production.Product` a
                left join `adventureworks2019.Production.WorkOrder` b using (ProductID)
                group by 1, 2, 3
                order by 1, 3, 2 desc)

, previous as (
                select
                  *
                  , lag(stock_qty) over(partition by Name order by  Name, yr, mth) stock_prv
                from stock_tab
                where stock_qty is not null
                      and yr = 2011
                order by Name, yr, mth desc)


-- Select final output
select
  *
  , coalesce(round((100*(stock_qty - stock_prv)/stock_prv), 1), 0) diff
from previous;


----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 7 ** Calc Ratio of Stock / Sales in 2011 by product name, by month. Order results by month desc, ratio desc
----------------------------------------------------------------------------------------------------------------------------------------------

with 
sale_info as (
  select 
      extract(month from a.ModifiedDate) as mth 
     , extract(year from a.ModifiedDate) as yr 
     , a.ProductId
     , b.Name
     , sum(a.OrderQty) as sales
  from `adventureworks2019.Sales.SalesOrderDetail` a 
  left join `adventureworks2019.Production.Product` b 
    on a.ProductID = b.ProductID
  where FORMAT_TIMESTAMP("%Y", a.ModifiedDate) = '2011'
  group by 1,2,3,4
), 

stock_info as (
  select
      extract(month from ModifiedDate) as mth 
      , extract(year from ModifiedDate) as yr 
      , ProductId
      , sum(StockedQty) as stock_cnt
  from 'adventureworks2019.Production.WorkOrder'
  where FORMAT_TIMESTAMP("%Y", ModifiedDate) = '2011'
  group by 1,2,3
)

select
      a.*
    , b.stock_cnt as stock  --(*)
    , round(coalesce(b.stock_cnt,0) / sales,2) as ratio
from sale_info a 
full join stock_info b 
  on a.ProductId = b.ProductId
and a.mth = b.mth 
and a.yr = b.yr
order by 1 desc, 7 desc;


----------------------------------------------------------------------------------------------------------------------------------------------
-- ** QUERY 8 ** No of order and value at Pending status in 2014
----------------------------------------------------------------------------------------------------------------------------------------------

select
  extract(year from date(ModifiedDate))  yr
  , Status
  , count(distinct PurchaseOrderID) as order_Cnt 
  , sum(TotalDue) as value
from `adventureworks2019.Purchasing.PurchaseOrderHeader`
where Status = 1
      and extract(year from date(ModifiedDate)) = 2014
group by 1, 2;


-----------------------------------------------
-- THE END - THANK YOU VERY MUCH FOR YOUR TIME
-----------------------------------------------
