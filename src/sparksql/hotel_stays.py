from pyspark.sql import SparkSession


from collections import namedtuple
import sys

AzureContainer = namedtuple('AzureContainer', ['accountName', 'accountKey', 'name'])

def run():
    sourceContainer = AzureContainer(
        accountName=sys.argv[1], 
        accountKey=sys.argv[2],
        name=sys.argv[3]
    )
    deltaHome = sys.argv[4]

    spark = createSession(sourceContainer)
    importFromSourceToLocal(spark, sourceContainer, deltaHome)

    print('Task #1:')
    maxTmprDiffHotels = spark.sql(f'''
        select id, any_value(name) name, any_value(address) address, any_value(city) city, any_value(country) country, 
            date_format(any_value(wthr_date), 'yyyy-MM') wthr_month, 
            max(avg_tmpr_c) - min(avg_tmpr_c) tmpr_diff_c,
            any_value(year) year, any_value(month) month
        from delta.`{deltaHome}/hotel_weather`
        group by id, trunc(wthr_date, 'month')
        order by tmpr_diff_c desc
        limit 10
    ''')
    maxTmprDiffHotels.show(truncate=False)
    maxTmprDiffHotels.write.format('delta').partitionBy('year', 'month').mode('overwrite').save(f'{deltaHome}/max_tmpr_diff_hotels')
    print()

    print('Task #2:')
    busyHotels = spark.sql(f'''
        select distinct hotel_id, name, address, city, country, date_format(srch_date, 'yyyy-MM') srch_month, srch_count,
            year(srch_date) year, month(srch_date) month
        from (
            select hotel_id, srch_date, count(srch_date) srch_count, row_number() over (partition by srch_date order by count(srch_date) desc) rn
            from (     
                select hotel_id, explode(sequence(trunc(srch_ci, 'month'), trunc(srch_co, 'month'), interval 1 month)) as srch_date
                from delta.`{deltaHome}/expedia`
                where srch_ci <= srch_co
            )
            group by hotel_id, srch_date
        ) visits 
            join delta.`{deltaHome}/hotel_weather` hw on visits.hotel_id = hw.id
        where rn <= 10
        order by srch_month, srch_count desc
    ''')
    busyHotels.show(truncate=False)
    busyHotels.write.format('delta').partitionBy('year', 'month').mode('overwrite').save(f'{deltaHome}/busy_hotels')
    print()

    print('Task #3:')
    extendedVisits = spark.sql(f'''
        select distinct hotel_id, srch_ci stay_start, srch_co stay_end, 
            avg_stay_tmpr_c, (last_day_tmpr_c - first_day_tmpr_c) tmpr_trend_c
        from (
            select hotel_id, srch_ci, srch_co,
                avg(avg_tmpr_c) over stay as avg_stay_tmpr_c,
                first_value(avg_tmpr_c) over stay as first_day_tmpr_c,
                last_value(avg_tmpr_c) over stay as last_day_tmpr_c
            from delta.`{deltaHome}/expedia` exp
                left join delta.`{deltaHome}/hotel_weather` hw on exp.hotel_id = hw.id
                    and wthr_date between srch_ci and srch_co
            where datediff(srch_co, srch_ci) > 7
            order by hotel_id, srch_ci, srch_co
            window stay as (
                partition by hotel_id, srch_ci, srch_co 
                order by wthr_date 
                rows between unbounded preceding and unbounded following)
        )
    ''')
    extendedVisits.show(truncate=False)
    extendedVisits.write.format('delta').mode('overwrite').save(f'{deltaHome}/extended_stays')

def createSession(storageContainer: AzureContainer) -> SparkSession:
    builder = SparkSession.Builder() \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = builder.getOrCreate()
    spark.conf.set(f'fs.azure.account.key.{storageContainer.accountName}.dfs.core.windows.net', storageContainer.accountKey)        
    return spark

def importFromSourceToLocal(spark: SparkSession, sourceContainer: AzureContainer, deltaHome: str):
    spark.sql(f'''
        create table if not exists delta.`{deltaHome}/hotel_weather`
        using delta
        as
        select *
        from parquet.`{getContainerPath(sourceContainer)}/hotel-weather`
    ''')
    spark.sql(f'''
        create table if not exists delta.`{deltaHome}/expedia`
        using delta
        as
        select *
        from avro.`{getContainerPath(sourceContainer)}/expedia`;
    ''')

def getContainerPath(container: AzureContainer):
    return f'abfss://{container.name}@{container.accountName}.dfs.core.windows.net'

if __name__ == '__main__':
    run()
