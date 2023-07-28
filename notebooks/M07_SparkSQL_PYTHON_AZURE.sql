-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create input parameters
-- MAGIC
-- MAGIC > Please don't forget to fill them with correct values!

-- COMMAND ----------

create widget text sourceStorageAccountName default "m07sparksql2023";
create widget text sourceStorageContainerName default "m07sparksql";

create widget text destStorageAccountName default "storsm0723westeurope";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create delta table for hotel/weather data

-- COMMAND ----------

create table if not exists hotel_weather;

copy into hotel_weather
from "abfss://${sourceStorageContainerName}@${sourceStorageAccountName}.dfs.core.windows.net/hotel-weather"
fileformat = parquet
copy_options ('mergeSchema' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create delta table for expedia data

-- COMMAND ----------

create table if not exists expedia;

copy into expedia
from 'abfss://${sourceStorageContainerName}@${sourceStorageAccountName}.dfs.core.windows.net/expedia'
fileformat = avro
copy_options ('mergeSchema' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Save joined data

-- COMMAND ----------

drop table if exists hotel_weather_searches;

create external table hotel_weather_searches
using parquet
location 'abfss://data@${destStorageAccountName}.dfs.core.windows.net/hotel_weather_searches'
partitioned by (year, month, day)
as 
select * except (hw.id)
from hotel_weather hw full join expedia exp on hw.id = exp.hotel_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Task 1
-- MAGIC
-- MAGIC Top 10 hotels with max absolute temperature difference by month

-- COMMAND ----------

drop table if exists max_tmpr_diff_hotels;

create external table max_tmpr_diff_hotels
using parquet
location 'abfss://data@${destStorageAccountName}.dfs.core.windows.net/max_tmpr_diff_hotels'
partitioned by (year, month)
as 
select id, any_value(name) name, any_value(address) address, any_value(city) city, any_value(country) country, 
    date_format(any_value(wthr_date), 'yyyy-MM') wthr_month, 
    max(avg_tmpr_c) - min(avg_tmpr_c) tmpr_diff_c,
    any_value(year) year, any_value(month) month
from hotel_weather
group by id, trunc(wthr_date, 'month')
order by tmpr_diff_c desc
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Task 1: Execution Plan
-- MAGIC
-- MAGIC ##### The plan:
-- MAGIC ```
-- MAGIC == Physical Plan ==
-- MAGIC AdaptiveSparkPlan (19)
-- MAGIC +- == Final Plan ==
-- MAGIC    TakeOrderedAndProject (11)
-- MAGIC    +- SortAggregate (10)
-- MAGIC       +- Sort (9)
-- MAGIC          +- AQEShuffleRead (8)
-- MAGIC             +- ShuffleQueryStage (7), Statistics(sizeInBytes=2.7 MiB, rowCount=1.02E+4, ColumnStat: N/A, isRuntime=true)
-- MAGIC                +- Exchange (6)
-- MAGIC                   +- SortAggregate (5)
-- MAGIC                      +- * Sort (4)
-- MAGIC                         +- * Project (3)
-- MAGIC                            +- * ColumnarToRow (2)
-- MAGIC                               +- Scan parquet spark_catalog.default.hotel_weather (1)
-- MAGIC +- == Initial Plan ==
-- MAGIC    TakeOrderedAndProject (18)
-- MAGIC    +- SortAggregate (17)
-- MAGIC       +- Sort (16)
-- MAGIC          +- Exchange (15)
-- MAGIC             +- SortAggregate (14)
-- MAGIC                +- Sort (13)
-- MAGIC                   +- Project (12)
-- MAGIC                      +- Scan parquet spark_catalog.default.hotel_weather (1)
-- MAGIC                      
-- MAGIC ```
-- MAGIC
-- MAGIC The most time-consuming part was code generation for the projecting (steps 2-4): 3.4 s.
-- MAGIC
-- MAGIC ##### Mappings:
-- MAGIC - `Scan parquet spark_catalog.default.hotel_weather`, `Project` and `Sort`
-- MAGIC    - Scanning the table and projecting the required columns, sorting by attributes of `group by`
-- MAGIC - `SortAggregate`
-- MAGIC    - Group and aggregate records by chosen columns
-- MAGIC - `TakeOrderedAndProject`
-- MAGIC    - Order the results by the specified column and limit them
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##### The plan:
-- MAGIC ```
-- MAGIC == Physical Plan ==
-- MAGIC AdaptiveSparkPlan (19)
-- MAGIC +- == Final Plan ==
-- MAGIC    TakeOrderedAndProject (11)
-- MAGIC    +- SortAggregate (10)
-- MAGIC       +- Sort (9)
-- MAGIC          +- AQEShuffleRead (8)
-- MAGIC             +- ShuffleQueryStage (7), Statistics(sizeInBytes=2.7 MiB, rowCount=1.02E+4, ColumnStat: N/A, isRuntime=true)
-- MAGIC                +- Exchange (6)
-- MAGIC                   +- SortAggregate (5)
-- MAGIC                      +- * Sort (4)
-- MAGIC                         +- * Project (3)
-- MAGIC                            +- * ColumnarToRow (2)
-- MAGIC                               +- Scan parquet spark_catalog.default.hotel_weather (1)
-- MAGIC +- == Initial Plan ==
-- MAGIC    TakeOrderedAndProject (18)
-- MAGIC    +- SortAggregate (17)
-- MAGIC       +- Sort (16)
-- MAGIC          +- Exchange (15)
-- MAGIC             +- SortAggregate (14)
-- MAGIC                +- Sort (13)
-- MAGIC                   +- Project (12)
-- MAGIC                      +- Scan parquet spark_catalog.default.hotel_weather (1)
-- MAGIC                      
-- MAGIC ```
-- MAGIC
-- MAGIC The most time-consuming parts was executing fused projecting steps (2-4): 3.4s.
-- MAGIC
-- MAGIC ##### Mappings:
-- MAGIC - `Scan parquet spark_catalog.default.hotel_weather`, `Project` and `Sort`
-- MAGIC    - Scan the table and project the required columns, sorting by attributes of `group by`
-- MAGIC - `SortAggregate`
-- MAGIC    - Group and aggregate records by chosen columns
-- MAGIC - `TakeOrderedAndProject`
-- MAGIC    - Order the results by the specified column and limit them
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Task 2
-- MAGIC
-- MAGIC Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.

-- COMMAND ----------

drop table if exists busy_hotels;

create table busy_hotels
using parquet
location 'abfss://data@storsm0723westeurope.dfs.core.windows.net/busy_hotels'
partitioned by (year, month)
as 
select distinct hotel_id, name, address, city, country, date_format(srch_date, 'yyyy-MM') srch_month, srch_count,
    year(srch_date) year, month(srch_date) month
from (
    select hotel_id, srch_date, count(srch_date) srch_count, row_number() over (partition by srch_date order by count(srch_date) desc) rn
    from (     
        select hotel_id, explode(sequence(trunc(srch_ci, 'month'), trunc(srch_co, 'month'), interval 1 month)) as srch_date
        from expedia
        where srch_ci <= srch_co
    )
    group by hotel_id, srch_date
) visits 
    join hotel_weather hw on visits.hotel_id = hw.id
where rn <= 10
order by srch_month, srch_count desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Task 2: Execution Plan
-- MAGIC
-- MAGIC ##### The plan:
-- MAGIC ```
-- MAGIC == Physical Plan ==
-- MAGIC AdaptiveSparkPlan (50)
-- MAGIC +- == Final Plan ==
-- MAGIC    TakeOrderedAndProject (30)
-- MAGIC    +- * HashAggregate (29)
-- MAGIC       +- AQEShuffleRead (28)
-- MAGIC          +- ShuffleQueryStage (27), Statistics(sizeInBytes=35.0 KiB, rowCount=203, ColumnStat: N/A, isRuntime=true)
-- MAGIC             +- Exchange (26)
-- MAGIC                +- * HashAggregate (25)
-- MAGIC                   +- * Project (24)
-- MAGIC                      +- * BroadcastHashJoin Inner BuildRight (23)
-- MAGIC                         :- * HashAggregate (17)
-- MAGIC                         :  +- * Project (16)
-- MAGIC                         :     +- * Filter (15)
-- MAGIC                         :        +- * RunningWindowFunction (14)
-- MAGIC                         :           +- * Sort (13)
-- MAGIC                         :              +- AQEShuffleRead (12)
-- MAGIC                         :                 +- ShuffleQueryStage (11), Statistics(sizeInBytes=518.2 KiB, rowCount=1.33E+4, ColumnStat: N/A, isRuntime=true)
-- MAGIC                         :                    +- Exchange (10)
-- MAGIC                         :                       +- * HashAggregate (9)
-- MAGIC                         :                          +- AQEShuffleRead (8)
-- MAGIC                         :                             +- ShuffleQueryStage (7), Statistics(sizeInBytes=1917.8 KiB, rowCount=6.14E+4, ColumnStat: N/A, isRuntime=true)
-- MAGIC                         :                                +- Exchange (6)
-- MAGIC                         :                                   +- * HashAggregate (5)
-- MAGIC                         :                                      +- * Generate (4)
-- MAGIC                         :                                         +- * Filter (3)
-- MAGIC                         :                                            +- * ColumnarToRow (2)
-- MAGIC                         :                                               +- Scan parquet spark_catalog.default.expedia (1)
-- MAGIC                         +- ShuffleQueryStage (22), Statistics(sizeInBytes=2000.5 KiB, rowCount=1.33E+4, ColumnStat: N/A, isRuntime=true)
-- MAGIC                            +- Exchange (21)
-- MAGIC                               +- * Filter (20)
-- MAGIC                                  +- * ColumnarToRow (19)
-- MAGIC                                     +- Scan parquet spark_catalog.default.hotel_weather (18)
-- MAGIC +- == Initial Plan ==
-- MAGIC    TakeOrderedAndProject (49)
-- MAGIC    +- HashAggregate (48)
-- MAGIC       +- Exchange (47)
-- MAGIC          +- HashAggregate (46)
-- MAGIC             +- Project (45)
-- MAGIC                +- BroadcastHashJoin Inner BuildRight (44)
-- MAGIC                   :- HashAggregate (41)
-- MAGIC                   :  +- Project (40)
-- MAGIC                   :     +- Filter (39)
-- MAGIC                   :        +- RunningWindowFunction (38)
-- MAGIC                   :           +- Sort (37)
-- MAGIC                   :              +- Exchange (36)
-- MAGIC                   :                 +- HashAggregate (35)
-- MAGIC                   :                    +- Exchange (34)
-- MAGIC                   :                       +- HashAggregate (33)
-- MAGIC                   :                          +- Generate (32)
-- MAGIC                   :                             +- Filter (31)
-- MAGIC                   :                                +- Scan parquet spark_catalog.default.expedia (1)
-- MAGIC                   +- Exchange (43)
-- MAGIC                      +- Filter (42)
-- MAGIC                         +- Scan parquet spark_catalog.default.hotel_weather (18) 
-- MAGIC                                       
-- MAGIC ```
-- MAGIC
-- MAGIC The most time-consuming part was executing fused projecting steps for reading `hotel_weather` table: 12.2s.
-- MAGIC
-- MAGIC ##### Mappings:
-- MAGIC - From `Scan parquet spark_catalog.default.expedia (1)` to `Filter (3)`
-- MAGIC    - Scan `expedia` table and project the required columns, filtering out nulls for check-in and check-out dates
-- MAGIC - `Generate (4)`
-- MAGIC    - Explode the check-in-check-out intervals
-- MAGIC - `HashAggregate (5), (9)`
-- MAGIC    - Group records in `expedia` table by hotel ID and year-month of a visit, then counting the records by the year-month
-- MAGIC - `Sort (13)` and `RunningWindowFunction (14)`
-- MAGIC    - Execute `row_number()` function over the records partitioned by year-month of visits
-- MAGIC - `Filter (15)` and `Project (16)`
-- MAGIC    - Project the results from `expedia` table, filter out nulls for hotel IDs and limit the results by row number
-- MAGIC - From `Scan parquet spark_catalog.default.hotel_weather (18)` to `ShuffleQueryStage (22)`
-- MAGIC    - Scan `hotel_weather` table and filter out nulls for hotel IDs
-- MAGIC - `BroadcastHashJoin Inner BuildRight (23)` and `Project (24)`
-- MAGIC    - Join the two tables by hotel IDs
-- MAGIC - `HashAggregate (25), (29)`
-- MAGIC    - Filter distinct rows
-- MAGIC - `TakeOrderedAndProject (30)`
-- MAGIC    - Order the results year-month of visits and visits count

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Task 3
-- MAGIC
-- MAGIC For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.

-- COMMAND ----------

drop table if exists extended_stays;

create external table extended_stays
using parquet
location 'abfss://data@storsm0723westeurope.dfs.core.windows.net/extended_stays'
as 
select distinct hotel_id, srch_ci stay_start, srch_co stay_end, 
    avg_stay_tmpr_c, (last_day_tmpr_c - first_day_tmpr_c) tmpr_trend_c
from (
    select hotel_id, srch_ci, srch_co,
        avg(avg_tmpr_c) over stay as avg_stay_tmpr_c,
        first_value(avg_tmpr_c) over stay as first_day_tmpr_c,
        last_value(avg_tmpr_c) over stay as last_day_tmpr_c
    from expedia exp
        left join hotel_weather hw on exp.hotel_id = hw.id
            and wthr_date between srch_ci and srch_co
    where datediff(srch_co, srch_ci) > 7
    order by hotel_id, srch_ci, srch_co
    window stay as (
        partition by hotel_id, srch_ci, srch_co 
        order by wthr_date 
        rows between unbounded preceding and unbounded following)
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Task 3: Execution Plan
-- MAGIC
-- MAGIC ##### The plan:
-- MAGIC ```
-- MAGIC == Physical Plan ==
-- MAGIC AdaptiveSparkPlan (28)
-- MAGIC +- == Final Plan ==
-- MAGIC    CollectLimit (17)
-- MAGIC    +- * HashAggregate (16)
-- MAGIC       +- Window (15)
-- MAGIC          +- Sort (14)
-- MAGIC             +- AQEShuffleRead (13)
-- MAGIC                +- ShuffleQueryStage (12), Statistics(sizeInBytes=5.4 MiB, rowCount=6.42E+4, ColumnStat: N/A, isRuntime=true)
-- MAGIC                   +- Exchange (11)
-- MAGIC                      +- * Project (10)
-- MAGIC                         +- * BroadcastHashJoin LeftOuter BuildRight (9)
-- MAGIC                            :- * Filter (3)
-- MAGIC                            :  +- * ColumnarToRow (2)
-- MAGIC                            :     +- Scan parquet spark_catalog.default.expedia (1)
-- MAGIC                            +- ShuffleQueryStage (8), Statistics(sizeInBytes=832.8 KiB, rowCount=1.33E+4, ColumnStat: N/A, isRuntime=true)
-- MAGIC                               +- Exchange (7)
-- MAGIC                                  +- * Filter (6)
-- MAGIC                                     +- * ColumnarToRow (5)
-- MAGIC                                        +- Scan parquet spark_catalog.default.hotel_weather (4)
-- MAGIC +- == Initial Plan ==
-- MAGIC    CollectLimit (27)
-- MAGIC    +- HashAggregate (26)
-- MAGIC       +- Window (25)
-- MAGIC          +- Sort (24)
-- MAGIC             +- Exchange (23)
-- MAGIC                +- Project (22)
-- MAGIC                   +- BroadcastHashJoin LeftOuter BuildRight (21)
-- MAGIC                      :- Filter (18)
-- MAGIC                      :  +- Scan parquet spark_catalog.default.expedia (1)
-- MAGIC                      +- Exchange (20)
-- MAGIC                         +- Filter (19)
-- MAGIC                            +- Scan parquet spark_catalog.default.hotel_weather (4)
-- MAGIC                                       
-- MAGIC ```
-- MAGIC
-- MAGIC The most time-consuming part was executing fused join and projecting steps: 3.7s.
-- MAGIC
-- MAGIC ##### Mappings:
-- MAGIC - From `Scan parquet spark_catalog.default.expedia (1)` to `Filter (3)`
-- MAGIC    - Scan `expedia` table and project the required columns, filtering out nulls for check-in and check-out dates and records with less than 7-day interval between the dates
-- MAGIC - From `Scan parquet spark_catalog.default.hotel_weather (4)` to `ShuffleQueryStage (8)`
-- MAGIC    - Scan `hotel_weather` table and filter out nulls for hotel IDs and observation dates (`wthr_date`)
-- MAGIC - `BroadcastHashJoin LeftOuter BuildRight (9)` and `Project (10)`
-- MAGIC    - Join the two tables
-- MAGIC - `Sort (14)`
-- MAGIC    - Sort the results of the join
-- MAGIC - `Window (15)`
-- MAGIC    - Apply aggregations to the records partitioned by the defined window
-- MAGIC - `HashAggregate (16)`
-- MAGIC    - Filter distinct rows
