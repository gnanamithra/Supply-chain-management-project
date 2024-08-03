show databases;
create database spc_analysis;
create table plugs_data(
order_num int,
order_date date,
sku_num varchar(260),
quantity int,
cost float,
price float,
product_type varchar(260),
product_family varchar(260),
product_name varchar(260),
store_name varchar(260),
storekey int,
store_region varchar(260),
store_state varchar(260),
store_city varchar(260),
store_latitude float,
store_longitude float,
cust_name varchar(260),
custkey int,
sales float);
desc plugs_data;

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/PLUGS_ELECTRONICS.csv'
into table plugs_data
fields terminated by ','
enclosed by '"' lines
terminated by '\n'
ignore 1 lines;

show global variables like 'local_infile';
set global local_infile=true;
select*from plugs_data;
set global local_infile=1;
show variables like "secure_file_priv";

-- top 5 stores 
select store_name,concat(round(sum(sales)/1000000,2)," ","M")  total_sales from plugs_data group by store_name order by total_sales desc limit 5;


-- Region wise sales
select store_region,concat(round(sum(sales)/1000000,2)," ","M")  total_sales from plugs_data group by store_region;

-- YOY change
with yoy as(select year(order_date) as year,
 monthname(order_date) as month,
 sum(sales) as sale
 from plugs_data
 group by
	year(order_date),
    monthname(order_date))
    select year,month,sale,concat(round(((sale-lag(sale) over(order by year))/nullif(lag(sale) over(order by year),0)*100)),'%') as yoy_change
    from yoy order by year;
  
  -- fsale
    create table fsales(
    ordernumber int,
    custkey int,
    storekey int,
    transactiontype varchar(260),
    date date,
    purchasemethod varchar(260));
    
load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/F_SALES.csv'
into table fsales
fields terminated by ','
enclosed by '"' lines
terminated by '\n'
ignore 1 lines;

show global variables like 'local_infile';
set global local_infile=true;
select*from fsales;

-- purchase method wise sales
select fsales.purchasemethod,concat(round(sum(plugs_data.sales)/1000000,2)," ","M") as total_sales from plugs_data 
inner join fsales  on (plugs_data.ordernumber=fsales.ordernumber) 
group by fsales.purchasemethod;

-- top 5 product wise sales    
select case when grouping(product_name) then "grand_total" else product_name end product_name, concat(round(sum(sales)/1000000,0),"M") as total from 
plugs_data group by product_name with rollup order by total desc limit 5;

-- YTD sales
select year(order_date) as year,concat(round(sum(sales)/1000000,0),"M") as total,
sum(concat(round(sum(sales)/1000000,0),"M")) over (order by year(order_date)) as YTD
from plugs_data
group by year(order_date)
;

-- MTD
select order_num,order_date,sales,
sum(sales) over(partition by month(order_date) order by order_date
rows between unbounded preceding and current row) as MTD
from plugs_data;

-- top 5 state wise sales
select store_state as state,concat(round(sum(sales)/1000000,2)," ","M")  total_sales from plugs_data 
group by store_state order by total_sales desc limit 5;    

-- total stock quantity
select product_type,sum(quantity) from plugs_data group by product_type;

-- qoq sales
with qoq as(select quarter(order_date) as quarter,
 monthname(order_date) as month,
 sum(sales) as sale
 from plugs_data
 group by
	quarter(order_date),
    monthname(order_date))
    select quarter,month,sale,concat(round(((sale-lag(sale) over(order by quarter))/nullif(lag(sale) over(order by quarter),0)*100)),'%') as qoq_change
    from qoq order by quarter;
    
-- product type wise sale
select case when grouping(product_type) then "grand_total" else product_type end product_type, concat(round(sum(sales)/1000000,0),"M") as total from 
plugs_data group by product_type with rollup;

-- day wise sales
select day(order_date) as day,concat(round(sum(sales)/1000000,0),"M") as total
from plugs_data
group by day(order_date)
order by day(order_date);	