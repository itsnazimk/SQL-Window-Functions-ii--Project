 -- WINDOW FUNCTIONS PRACTICAL PROJECT 2 -- 
 -- car sales -- 
 
 create database if not exists wd_db ;
 use wd_db ;
 
 create table car_sales (
	car_Name  varchar(50),
 	car_year varchar(50),
	selling_price int ,
	km_driven int ,
	fuel varchar(50),
	seller_type varchar(50),
	transmission varchar(50),
	car_owner varchar(50),
	mileage  float ,
	car_engine int ,
	max_power int ,
	seats int ) ;
    
-- importing csv
select * from car_sales ;
 

--  ROW_NUMBER(), RANK(), DENSE_RANK(), and basic OVER() --

## 1. 	Assign a row number to each car ordered by selling price (highest first).
select car_name , selling_price , row_number() over(order by selling_price desc) as row_num from car_sales;
select * ,row_number() over(order by selling_price desc) as row_num from car_sales;

## 2. Rank cars by mileage within each fuel type.
select car_name , fuel , mileage , rank() over(partition by fuel order by mileage desc)as mileage_rank from car_sales; 

## 3. 	Assign a dense rank to cars based on year (newer first).
select car_name , car_year , dense_rank() over(order by car_year desc)as year_rank from car_sales ;

## 4. Add row numbers partitioned by transmission type over years.
select car_name , transmission , row_number() over(partition by transmission order by car_year desc)as row_num_by_transmission from car_sales; 

## 5. 	Show the top 1 latest car per seller_type.
select * from 
(select *, row_number() over(partition by seller_type order by car_year desc) as rank_by_transmission from car_sales) as rn1
where rank_by_transmission = 1 ;

## 6. 	Rank cars based on km_driven within owner category.
select car_name , car_owner, km_driven , rank() over(partition by car_owner order by km_driven desc) as rnk from car_sales;

## 7. 	Get the oldest car per brand as per car driven (use substring to extract brand).
select * from
(select car_name ,car_year, km_driven , rank() over(partition by substring_index(car_name , " " , 1) order by car_year asc) as rn from car_sales) as tt 
where rn = 1;

## 8. 	Assign dense rank based on seats.
select * , dense_rank() over(order by seats desc) from car_sales;
select car_name , seats  , dense_rank() over(order by seats desc)as seat_rank from car_sales;

## 9. 	Find the latest model per fuel type.
select * from 
(select car_name , car_year , fuel , row_number() over(partition by fuel order by car_year desc) as rn_fuel_year from car_sales) as tt
where rn_fuel_year = 1 ;

## 10. 	Find the cheapest car per transmission type.
select * from
(select car_name , transmission , selling_price , rank() over(partition by  transmission order by selling_price)as rn from car_sales)as tt
where rn = 1 ;

## 11. Show cars and their overall rank by price.
select car_name , selling_price , rank() over(order by selling_price desc) as rn from car_sales;

## 12. 	Assign row numbers to all cars grouped by fuel and ordered by power.
select car_name , fuel , max_power , row_number() over(partition by fuel order by max_power desc) as rn from car_sales;

## 12(i) Assign row numbers to all cars grouped by fuel and ordered by power and then show top 3
select * from 
(select car_name , fuel , max_power , row_number() over(partition by fuel order by max_power desc) as rn from car_sales) as tt 
where rn < 4 ;


--  LAG(), LEAD(), NTILE(), cumulative sums, and moving averages --

## 1. Find previous car's price for each row (using lag).
select car_name , selling_price , lag(selling_price) over(order by selling_price ) as prev_price from car_sales ;

## 2.	Get next car's km_driven (using LEAD).
select car_name , km_driven , lead(km_driven) over(order by km_driven )as next_km  from car_sales ;

## 3. Calculate cumulative selling price by fuel type.
select car_name ,fuel , selling_price , sum(selling_price) over(partition by fuel order by selling_price) as cumulative_price from car_sales ;

## 4. Compute running average of price by seller_type.
select car_name , seller_type , selling_price , avg(selling_price) over(partition by seller_type order by selling_price) as cum_avg_price from car_sales ; 

## 5. 	Divide cars into 4 price buckets using NTILE.
select car_name , selling_price , ntile(4) over(order by selling_price) as price_bucket from car_sales ;  

## 6. Compare mileage difference with previous car by year.
select car_name , car_year , mileage , mileage -lag(mileage) over(partition by car_year order by mileage) as mileage_diff from car_sales ; 

## 7.  Show the top 3 mileage cars per owner type.
select * from 
(select car_name , car_owner , mileage , row_number() over(partition by car_owner order by mileage desc)as rnk_mileage from car_sales) as tb
where rnk_mileage = 1 ; 

## 8. 	Show cumulative km driven for each transmission type.
select car_name , transmission , km_driven , sum(km_driven) over(partition by transmission order by km_driven desc) as running_km from car_sales ;
select * from car_sales ; 
 
 
 ## 8(i). 	Show highest cumulative km driven for each transmission type.
SELECT *
from 
( select * ,
    RANK() OVER (PARTITION BY transmission ORDER BY running_km DESC) AS rnk
FROM (
    SELECT 
        car_name, 
        transmission, 
        km_driven,
        SUM(km_driven) OVER (PARTITION BY transmission ORDER BY km_driven DESC) AS running_km
    FROM car_sales
) AS t ) as ranked
where rnk = 1 ;

## 9. Compute difference between consecutive prices.
select car_name , selling_price ,  selling_price - lag(selling_price) over(order by selling_price) as diff from car_sales ;

## 10. Calculate 3-car moving average or rolling average of mileage by fuel.
-- a moving average  is calculated using the "AVG()" window function with a "ROWS between _ preceding and current row" clause
select car_name , fuel , mileage , avg(mileage) over(partition by fuel order by mileage rows between 2 preceding and current row) as moving_avg   -- as we want a 3-row moving average (current + 2 previous rows):
from car_sales;     -- as we want a 3-row moving average (current + 2 previous rows):

## 11. Rank cars by engine size .
select car_name , car_engine , rank() over(order by car_engine desc) as engine_rank from car_sales;  

## 12.  Show change in max_power between consecutive cars.
select car_name , max_power , max_power - lead(max_power) over(order by max_power desc) as diff_power from car_sales ;

## 13. Compare selling_price and show if it’s increasing or decreasing.
select car_name , selling_price , car_year ,
CASE WHEN selling_price >  selling_price - lead(selling_price) over(order by car_year desc) THEN "Increase" else "Decrease" END as Trend from car_sales ;

-- END -- 
