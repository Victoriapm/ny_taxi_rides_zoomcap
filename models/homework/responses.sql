-----------------------
----- Question 1 ------
-----------------------
--- how it works
select extract(year from pickup_datetime) as pu_year, 
count(*) as cuenta
from {{ ref('fact_trips') }}
where  extract(year from pickup_datetime) in (2019, 2020)
group by 1

-- the answer
select count(*) as cuenta
from {{ ref('fact_trips') }}
where  extract(year from pickup_datetime) in (2019, 2020)

-----------------------
----- Question 2 ------
-----------------------
--- how it works
select service_type,  
count(*) as cuenta,
sum(count(*)) over () as total
from {{ ref('fact_trips') }}
where  extract(year from pickup_datetime) in (2019, 2020)
group by 1

-- the answer
select service_type, 
count(*) *100 / sum(count(*)) over () as distribution
from {{ ref('fact_trips') }}
where  extract(year from pickup_datetime) in (2019, 2020)
group by 1


-----------------------
----- Question 3 ------
-----------------------
--- the answer
select count(*) as cuenta
from {{ ref('stg_fhv_tripdata') }}
where  extract(year from pickup_datetime) in (2019)

-----------------------
----- Question 4 ------
-----------------------
--- the answer
select count(*) as cuenta
from {{ ref('fact_fhv_trips') }}
where  extract(year from pickup_datetime) in (2019)

-----------------------
----- Question 5 ------
-----------------------
--- the answer
select extract(month from pickup_datetime) as pu_month, 
count(*) as cuenta
from {{ ref('fact_fhv_trips') }}
where  extract(year from pickup_datetime) in (2019, 2020)
group by 1
order by cuenta desc
-- limit 1
