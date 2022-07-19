#!/usr/bin/env python
# coding: utf-8

# In[47]:


from pyspark.sql import SparkSession


# In[48]:


MAX_MEMORY = '5g'
spark = SparkSession.builder.appName('taxi-fare-prediction')                    .config('spark.executor.memory', MAX_MEMORY)                    .config('spark.driver.memory', MAX_MEMORY)                    .getOrCreate()


# # 데이터프레임 생성

# In[118]:


trip_files = '/Users/dongwoo/new_york/data/trips/*' # 모든 파일을 가져온다.
zone_file = '/Users/dongwoo/new_york/data/taxi+_zone_lookup.csv' 


# In[119]:


trips_df = spark.read.parquet(f"file:///{trip_files}", inferSchema=True, header=True)
zone_df = spark.read.csv(f"file:///{zone_file}", inferSchema=True, header=True)


# ### 스키마 생성

# In[120]:


trips_df.printSchema()


# In[121]:


trips_df.createOrReplaceTempView('trips')


# In[122]:


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


# In[123]:


data_df.printSchema()


# In[124]:


data_df.show()


# 데이터 타입 변경
# [참고자료](https://stackoverflow.com/questions/46956026/how-to-convert-column-with-string-type-to-int-form-in-pyspark-data-frame)

# In[125]:


# 데이터 타입 변경 (추후 전처리를 위해 숫자형 데이터 인 컬럼은 변경)
from pyspark.sql.types import IntegerType
data_df = data_df.withColumn("passenger_count", data_df["passenger_count"].cast(IntegerType()))
data_df = data_df.withColumn("pickup_location_id", data_df["pickup_location_id"].cast(IntegerType()))
data_df = data_df.withColumn("dropoff_location_id", data_df["dropoff_location_id"].cast(IntegerType()))


# In[126]:


data_df.printSchema()


# # Data Split

# In[127]:


train_df, test_df = data_df.randomSplit([0.8, 0.2], seed=1)


# In[128]:


print("train : ",train_df.count())
print("test : ",test_df.count())


# ### 데이터 저장
# - 추후 데이터를 가져오기 시간 단축을 위함

# In[129]:


data_dir = "/Users/dongwoo/new_york/data"


# In[133]:


# 데이터 저장하기
train_df.write.format("parquet").save(f"{data_dir}/train/")
test_df.write.format("parquet").save(f"{data_dir}/test/")


# In[134]:


# 데이터 불러오기
train_df = spark.read.parquet(f"{data_dir}/train/")
test_df = spark.read.parquet(f"{data_dir}/test/")


# In[135]:


train_df.printSchema()


# # Data Preprocessing

# ### One Hot Encoding & String Indexer
# > Categorical Data를 벡터화 합니다. 
# 
# - String Indexer는 `inputCol`컬럼을 인덱싱하여 `OutputCol` 컬럼을 새로 만드는 것이다.
# - 레이블 빈도에 따라 정렬되므로 가장 빈번한 레이블은 0번째 인덱스를 갖는다.
# - `.setHandleInvalid("keep")`은 변환 중 해당하지 않은 값이 들어올 떄 문제를 대처하는 역할을 한다. "keep", "error" or "skip"
# - [참고자료](https://knight76.tistory.com/entry/spark-머신-러닝-StringIndexer-예)
# 
# 
# `pickup_location_id` >> `pickup_location_id_index` >> **`pickup_location_id_onehot`**

# In[136]:


from pyspark.ml.feature import OneHotEncoder, StringIndexer

cat_feats = [
    "pickup_location_id",
    "dropoff_location_id",
    "day_of_week"
]

stages = []

for c in cat_feats:
    cat_indexer = StringIndexer(inputCol = c, outputCol = c + "_idex").setHandleInvalid("keep") 
    onehot_encoder = OneHotEncoder(inputCols=[cat_indexer.getOutputCol()], outputCols=[c + "_onehot"])
    
    stages += [cat_indexer, onehot_encoder] 


# In[137]:


stages


# ### Vector Assembler & StandardScaler
# > Numerical Data를 벡터화 합니다.
# 
# 
# `passenger_count` >> `passenger_count_vector` >> **`passenger_count_scaled`**

# In[138]:


from pyspark.ml.feature import VectorAssembler, StandardScaler

num_feats = [
    "passenger_count",
    "trip_distance",
    "pickup_time"
]

for n in num_feats:
    num_assembler = VectorAssembler(inputCols=[n], outputCol= n + "_vector")
    num_scaler = StandardScaler(inputCol=num_assembler.getOutputCol(), outputCol= n + "_scaled")
    
    stages += [num_assembler, num_scaler]


# In[139]:


stages


# ### 두 벡터화 된 컬럼을 다시 벡터화로 합친다.
# 
# `passenger_count_scaled` + `passenger_count_scaled` >> **`feature_vector`**

# In[140]:


assembler_inputs = [c + "_onehot" for c in cat_feats] + [n + "_scaled" for n in num_feats]
assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="feature_vector")
stages += [assembler]


# In[141]:


stages


# In[142]:


from pyspark.ml import Pipeline

tf_stages = stages # Processing을 완료한 stages
pipe = Pipeline(stages=tf_stages) # 파이프라인을 구축

tf_fit = pipe.fit(train_df) # fit


# In[143]:


v_train_df = tf_fit.transform(train_df) # transform


# In[144]:


v_train_df.printSchema()


# # Regression Modeling

# In[145]:


from pyspark.ml.regression import LinearRegression


# In[146]:


lr = LinearRegression(
    maxIter=5, # 반복수
    solver="normal",
    labelCol="total_amount", # Label(Traget)
    featuresCol='feature_vector' # 전처리 후 > feature -> feature_vector
    )


# In[147]:


model = lr.fit(v_train_df)


# In[148]:


vtest_df = tf_fit.transform(test_df) # test 데이터는 fit을 하지 않습니다. 


# In[149]:


prediction = model.transform(vtest_df)


# In[150]:


prediction.show()


# In[151]:


prediction.cache()


# In[152]:


prediction.select(["trip_distance", "day_of_week","total_amount","prediction"]).show()


# ## Evaluation

# In[153]:


model.summary.rootMeanSquaredError 


# In[154]:


model.summary.r2


# - R2 : 80%가 나온것으로 봤을때, 지난 성능보다 향상됬다고 볼 수 있다.
