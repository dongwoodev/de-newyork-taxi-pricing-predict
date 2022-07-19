from pyspark.sql import SparkSession

MAX_MEMORY = '5g'
spark = SparkSession.builder.appName('taxi-fare-prediction')\
                .config('spark.executor.memory', MAX_MEMORY)\
                .config('spark.driver.memory', MAX_MEMORY)\
                .getOrCreate()

# Create DataFrame
trip_files = '/Users/dongwoo/new_york/data/trips/*' # 모든 파일을 가져온다.
zone_file = '/Users/dongwoo/new_york/data/taxi+_zone_lookup.csv' 

trips_df = spark.read.parquet(f"file:///{trip_files}", inferSchema=True, header=True)
zone_df = spark.read.csv(f"file:///{zone_file}", inferSchema=True, header=True)

# Create Schema & Data Filtering
trips_df.createOrReplaceTempView('trips')
query = """
SELECT
    passenger_count,
    PULocationID as pickup_location_id,
    DOLocationID as dropoff_location_id,
    trip_distance,
    HOUR(tpep_pickup_datetime) as pickup_time,
    DATE_FORMAT(TO_DATE(tpep_pickup_datetime), 'EEEE') AS day_of_week,
    total_amount
FROM
    trips
WHERE
    total_amount < 5000
    AND total_amount > 0
    AND trip_distance > 0
    AND trip_distance < 500
    AND passenger_count < 4
    AND TO_DATE(tpep_pickup_datetime) >= '2021-01-01'
    AND TO_DATE(tpep_pickup_datetime) < '2022-01-01'
"""
data_df = spark.sql(query)
data_df.createOrReplaceTempView('df')

# 데이터 타입 변경 (추후 전처리를 위해 숫자형 데이터 인 컬럼은 변경)
from pyspark.sql.types import IntegerType
data_df = data_df.withColumn("passenger_count", data_df["passenger_count"].cast(IntegerType()))
data_df = data_df.withColumn("pickup_location_id", data_df["pickup_location_id"].cast(IntegerType()))
data_df = data_df.withColumn("dropoff_location_id", data_df["dropoff_location_id"].cast(IntegerType()))


# Data Split & Save
## `overwrite`모드를 통해 계속 실행동안 리프레쉬 한다.
train_df, test_df = data_df.randomSplit([0.8, 0.2], seed=1)
data_dir = "/Users/dongwoo/new_york/data"
train_df.write.format("parquet").mode('overwrite').save(f"{data_dir}/train/")
test_df.write.format("parquet").mode('overwrite').save(f"{data_dir}/test/")