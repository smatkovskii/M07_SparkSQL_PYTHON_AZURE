from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, ArrayType
import pyspark.sql.functions as F

import delta

from collections import namedtuple
import os

AzureContainer = namedtuple('AzureContainer', ['accountName', 'accountKey', 'name'])

sourceContainer = AzureContainer(
    accountName=os.environ['SOURCE_AZURE_STORAGE_ACCOUNT_NAME'], 
    accountKey=os.environ['SOURCE_AZURE_STORAGE_ACCOUNT_KEY'],
    name=os.environ['SOURCE_AZURE_STORAGE_CONTAINER_NAME']
)
destContainer = AzureContainer(
    accountName=os.environ['DEST_AZURE_STORAGE_ACCOUNT_NAME'], 
    accountKey=os.environ['DEST_AZURE_STORAGE_ACCOUNT_KEY'],
    name=os.environ['DEST_AZURE_STORAGE_CONTAINER_NAME']
)

def importDeltaSql():
    deltaHome = '/home/gene/downloads/m07_sql'
    spark = createSession(sourceContainer, destContainer)
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



    res1 = spark.sql(f'''
        select *
        from parquet.`{getContainerPath(destContainer)}/max_tmpr_diff_hotels`
    ''')
    res1.printSchema()
    res1.show()
    print(res1.count())

    res2 = spark.sql(f'''
        select *
        from parquet.`{getContainerPath(destContainer)}/busy_hotels`
    ''')
    res2.printSchema()
    res2.show()
    print(res2.count())

    res3 = spark.sql(f'''
        select *
        from parquet.`{getContainerPath(destContainer)}/extended_stays`
    ''')
    res3.printSchema()
    res3.show()
    print(res3.count())



    # maxTmprDiffHotels = spark.sql(f'''
    #     select id, any_value(name) name, any_value(address) address, any_value(city) city, any_value(country) country, 
    #         date_format(any_value(wthr_date), 'yyyy-MM') wthr_month, 
    #         max(avg_tmpr_c) - min(avg_tmpr_c) tmpr_diff_c
    #     from delta.`{deltaHome}/hotel_weather`
    #     group by id, trunc(wthr_date, 'month')
    #     order by tmpr_diff_c desc
    #     limit 10
    # ''')
    # maxTmprDiffHotels.show(truncate=False)
    # print(maxTmprDiffHotels.count())

    # busyHotels = spark.sql(f'''
    #     select distinct hotel_id, name, address, city, country, date_format(srch_date, 'yyyy-MM') srch_month, srch_count
    #     from (
    #         select hotel_id, srch_date, count(srch_date) srch_count, row_number() over (partition by srch_date order by count(srch_date) desc) rn
    #         from (     
    #             select hotel_id, explode(sequence(trunc(srch_ci, 'month'), trunc(srch_co, 'month'), interval 1 month)) as srch_date
    #             from delta.`{deltaHome}/expedia`
    #             where srch_ci <= srch_co
    #         )
    #         group by hotel_id, srch_date
    #     ) visits 
    #         join delta.`{deltaHome}/hotel_weather` hw on visits.hotel_id = hw.id
    #     where rn <= 10
    #     order by srch_month, srch_count desc
    # ''')
    # busyHotels.show(truncate=False)
    # print(busyHotels.count())

    # extendedVisits = spark.sql(f'''
    #     select distinct hotel_id, srch_ci stay_start, srch_co stay_end, 
    #         avg_stay_tmpr_c, (last_day_tmpr_c - first_day_tmpr_c) tmpr_trend_c
    #     from (
    #         select hotel_id, srch_ci, srch_co,
    #             avg(avg_tmpr_c) over stay as avg_stay_tmpr_c,
    #             first_value(avg_tmpr_c) over stay as first_day_tmpr_c,
    #             last_value(avg_tmpr_c) over stay as last_day_tmpr_c
    #         from delta.`{deltaHome}/expedia` exp
    #             left join delta.`{deltaHome}/hotel_weather` hw on exp.hotel_id = hw.id
    #                 and wthr_date between srch_ci and srch_co
    #         where datediff(srch_co, srch_ci) > 7
    #         order by hotel_id, srch_ci, srch_co
    #         window stay as (
    #             partition by hotel_id, srch_ci, srch_co 
    #             order by wthr_date 
    #             rows between unbounded preceding and unbounded following)
    #     )
    # ''')
    # extendedVisits.show(truncate=False)
    # print(extendedVisits.count())

    # spark.sql(f'''
    #     select *
    #     from delta.`{deltaHome}/hotel_weather`
    # ''').printSchema()

    # spark.sql(f'''
    #     select *
    #     from delta.`{deltaHome}/expedia`
    # ''').printSchema()

    #df.show(truncate=False, n=312)
    #print(df.count())
    #df.printSchema()

def readDelta():
    spark = createSession()
    # hotelWeather = spark.read.format('delta').option('versionAsOf', 0).load('/home/gene/downloads/m07/hotel_weather')
    # hotelWeather.printSchema()
    # hotelWeather.show()
    spark.sql(
        '''
        SELECT *
        FROM delta.`/home/gene/downloads/m07_sql/hotel_weather`
        '''
    ).show()

def createSession(*storageContainers: AzureContainer) -> SparkSession:
    builder = SparkSession.Builder() \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = builder.getOrCreate()
    for storage in storageContainers:
        spark.conf.set(f'fs.azure.account.key.{storage.accountName}.dfs.core.windows.net', storage.accountKey)
    return spark

def getContainerPath(container: AzureContainer):
    return f'abfss://{container.name}@{container.accountName}.dfs.core.windows.net'


####################
# TODO remove
importDeltaSql()