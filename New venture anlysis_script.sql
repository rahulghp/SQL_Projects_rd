use case1;
select* from weekly_sales limit 10;
/*In a single query, perform the following operations and generate a new table in
the open_mart schema named clean_weekly_sales:
Add a week_number as the second column for each week_date value, for
example any value from the 1st of January to 7th of January will be 1, 8th to 14th will be 2, etc.
Add a month_number with the calendar month for each week_date value as the 3rd column
Add a calendar_year column as the 4th column containing either 2018, 2019 or 2020 values
Add a new column called age_band after the original segment column using 
the following mapping on the number inside the segment value
Add a new demographic column using the following mapping for the first
letter in the segment values:
segment | demographic |
C | Couples |
F | Families |
Ensure all null string values with an "unknown" string value in the
original segment column as well as the
new age_band and demographic columns
Generate a new avg_transaction column as the sales value divided
by transactions rounded to 2 decimal places for each record*/


############################################# Data Cleaning ############################################

create table clean_weekly_sale as 
select week_date,
week(week_date) as week_number,
month(week_date) as month_number,
year(week_date) as calender_year,
region,
platform,
case when segment='null' then 'Unknown'
else segment
end as segment,
case
when right(segment,1)='1' then 'Young Adults'
when right(segment,1)='2'then 'Middle aged'
when right(segment,1) in ('3','4') then 'Retirees'
else 'Unknown'
end as age_band,
case 
when left(segment,1)='c' then 'Couples'
when left(segment,1)='F' then 'Families'
else 'Unknown'
end as demographic,
customer_type,transactions,sales,
round(sales/transactions,2) as 'avg_transaction'
from weekly_sales;
select * from clean_weekly_sale limit 10;


##Q1.Which week numbers are missing from the dataset?
create table seq100(x int auto_increment primary key);
insert into seq100 values(),(),(),(),(),(),(),(),(),();
insert into seq100 values(),(),(),(),(),(),(),(),(),();
insert into seq100 values(),(),(),(),(),(),(),(),(),();
insert into seq100 values(),(),(),(),(),(),(),(),(),();
insert into seq100 values(),(),(),(),(),(),(),(),(),();
insert into seq100 select x+50 from seq100;
select * from seq100;
create table seq52 as(select x from seq100 limit 52);
select * from seq52;
select distinct x as week_day from seq52
where x not in(select distinct week_number from clean_weekly_sale);
select distinct week_number from clean_weekly_sale;


# Q2.How many total transactions were there for each year in the dataset?
select calender_year,sum(transactions)
as total_transaction from clean_weekly_sale
group by calender_year;

# Q3.What are the total sales for each region for each month?
select region,month_number,sum(sales)
 as total_sales from clean_weekly_sale
 group by region,month_number
 order by total_sales desc;
 

## Q4.Month with maximum sales for each year and region?
WITH ranked_sales AS 
(
SELECT 
month_number, calender_year, sum(sales)as monthly_sales, region,
RANK() OVER (PARTITION BY region, calender_year ORDER BY sum(sales) desc) as sales_rank
FROM clean_weekly_sale
group by month_number,calender_year,region
order by monthly_sales
)
SELECT 
region,calender_year,month_number,monthly_sales
FROM ranked_sales
WHERE sales_rank = 1
group by month_number,calender_year,region
order by monthly_sales desc;


## Q5. Which year most reach max sales for each retail and shopify platform?
with max_sales_cte as
(select platform,calender_year,sum(sales)as total_sales,
row_number()over(partition by platform order by sum(sales))as rnk
from clean_weekly_sale
group by platform,calender_year)
select platform,calender_year,total_sales
from max_sales_cte
where rnk=1
group by platform,calender_year;



########################################data exploration##########################################




##Q6.What is the total count of transactions for each platform?
select platform,count(transactions) 
as total_transactions from clean_weekly_sale
group by platform;


##Q7.What is the percentage of sales for Retail vs Shopify for each month?
with percentage_of_sales_cte as 
(select calender_year,
month_number,
platform,
sum(sales) as monthly_sales
from clean_weekly_sale 
group by calender_year,month_number,platform)

select calender_year, month_number,
round(100*max(case when platform='Retail'then
monthly_sales else null end)/sum(monthly_sales),2)
as Retail_percentage,
round(100*max(case when platform='Shopify'
then monthly_sales else null end)/sum(monthly_sales),2)
as Shopify_percentage
from percentage_of_sales_cte
group by calender_year,month_number;
 
 
##Q8. Max yearly and monthly sales for each platform?
with max_sales_cte as
(select platform,calender_year,month_number,sum(sales)as total_sales,
row_number()over(partition by platform order by sum(sales)desc)as rnk
from clean_weekly_sale
group by platform,calender_year,month_number)
select platform,calender_year,month_number,total_sales
from max_sales_cte
where rnk=1
group by platform,calender_year,month_number;


##Q9.Total yearly sales by each platform?(for more detailing)
select calender_year,platform,sum(sales)as total_sales,
row_number()over(partition by calender_year order by sum(sales) desc) as sales_sank
from clean_weekly_sale
group by calender_year,platform;


##Q10.Monthly sales by each platform?(for more detailing)
select month_number,platform,sum(sales)as total_sales,
row_number()over(partition by month_number order by sum(sales) desc) as sales_rank
from clean_weekly_sale
group by month_number,platform;


##Q11.What is the percentage of sales by demographic for each year in the dataset?
with percentage_of_sales_cte as
(select calender_year,
month_number,
demographic,
sum(sales) as monthly_sales
from clean_weekly_sale 
group by calender_year,
month_number,
demographic)
select calender_year,
month_number,
round(100*max(case when demographic='Couples' 
then monthly_sales else null end)/sum(monthly_sales),2)
as couple_sale_percentage,
round(100*max(case when demographic='Families'
then monthly_sales else null end)/sum(monthly_sales),2)
as family_sale_percentage
from percentage_of_sales_cte
group by calender_year,month_number;

##Q12.Demographic most contributed to sales in each region?
select region,demographic,sum(sales) as total_sales,
row_number()over(partition by region order by sum(sales) desc) as slaes_rnk
from clean_weekly_sale
group by region,demographic;

##Q11.Total yearly Sales by demographic?
select calender_year,demographic,sum(sales)as total_sales,
row_number()over(partition by calender_year 
order by sum(sales) desc) as sales_rank
from clean_weekly_sale
group by calender_year,demographic;



##Q13.Total monthly Sales by demographic?
select month_number,demographic,sum(sales)as total_sales,
row_number()over(partition by month_number order by sum(sales) desc) as sales_sank
from clean_weekly_sale
group by month_number,demographic;


##Q13.Which age_band and demographic values contribute the most to Retail sales?
select demographic,age_band,sum(sales) 
as total_sales,
row_number()over
(partition by demographic 
order by sum(sales) desc) as rnk
from clean_weekly_sale 
where platform='Retail'
group by demographic,age_band;


##Q14. Which age_band and demographic values contribute the most to Shopify sales?
select demographic,age_band,sum(sales)
 as total_sales,
row_number()over(partition by demographic
order by sum(sales) desc) as rnk
from clean_weekly_sale 
where platform='Shopify'
group by demographic,age_band;



##Q15. People contribution on sales for each region as well as each platform?
select region,platform,age_band,sum(sales) as total_sales,
row_number()over(partition by region order by sum(sales) desc) as slaes_rnk
from clean_weekly_sale group by region,platform,age_band;


##Q16.Total yearly sales for each age_band?
select calender_year,age_band,
sum(sales)as total_sales,
row_number()over(partition by calender_year 
order by sum(sales) desc) as sales_rank
from clean_weekly_sale
group by calender_year,age_band;


##Q17.monthly sales for each age_band?
select month_number,age_band,sum(sales)as total_sales,
row_number()over(partition by month_number order by sum(sales) desc) as sales_sank
from clean_weekly_sale
group by month_number,age_band;


##Q18.customer type wise sales for each year?
select calender_year,
customer_type,sum(sales)
as total_sales,
row_number()over(partition by calender_year
order by sum(sales)desc) as sales_rnk
from clean_weekly_sale 
group by calender_year,customer_type;


##Q19.customer type wise sales for each platform?
select platform,
customer_type,
sum(sales) as total_sales,
row_number()over(partition by platform 
order by sum(sales)desc)as rnk
from clean_weekly_sale
group by platform,customer_type;


##Q20.in each region customer type wise sales for both platform?
select region,platform,customer_type,sum(sales) as total_sales,
row_number()over(partition by region,platform order by sum(sales) desc)as rnk
from clean_weekly_sale group by region,platform,customer_type;