#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession


# In[2]:


MAX_MEMORY = '5g'
spark = SparkSession.builder.appName('taxi-fare-prediction')                    .config('spark.executor.memory', MAX_MEMORY)                    .config('spark.driver.memory', MAX_MEMORY)                    .getOrCreate()


# # 데이터프레임 생성

# In[3]:


trip_files = '/Users/dongwoo/new_york/data/trips/*' # 모든 파일을 가져온다.
zone_file = '/Users/dongwoo/new_york/data/taxi+_zone_lookup.csv' 


# In[4]:


trips_df = spark.read.parquet(f"file:///{trip_files}", inferSchema=True, header=True)
zone_df = spark.read.csv(f"file:///{zone_file}", inferSchema=True, header=True)


# ### 스키마 생성

# In[5]:


trips_df.printSchema()


# In[7]:


trips_df.createOrReplaceTempView('trips')


# In[8]:


query = """
SELECT
    trip_distance,
    total_amount
FROM
    trips
WHERE
    total_amount < 5000
    AND total_amount > 0
    AND trip_distance < 500
    AND passenger_count < 5
    AND TO_DATE(tpep_pickup_datetime) >= '2021-01-01'
    AND TO_DATE(tpep_pickup_datetime) < '2022-01-01'

"""
data_df = spark.sql(query)
data_df.createOrReplaceTempView('df')


# In[9]:


data_df.show()


# In[10]:


data_df.describe().show()


# - 평균 3.0 마일 정도 거리를 가고 19불 정도 금액을 평균적으로 내는 것으로 보인다.

# # Data Split

# In[13]:


train_df, test_df = data_df.randomSplit([0.8, 0.2], seed=1)


# In[16]:


print("train : ",train_df.count())
print("test : ",test_df.count())


# In[17]:


from pyspark.ml.feature import VectorAssembler


# In[20]:


vassembler = VectorAssembler(inputCols=["trip_distance"], outputCol="features")
# trip_distance -> features로 들어가게됨.


# In[21]:


vtrain_df = vassembler.transform(train_df)


# In[22]:


vtrain_df.show()


# # Regression Modeling

# In[23]:


from pyspark.ml.regression import LinearRegression


# In[25]:


lr = LinearRegression(
    maxIter=50, # 반복수
    labelCol="total_amount", # Label(Traget)
    featuresCol='features' # features
    )


# In[26]:


model = lr.fit(vtrain_df)


# In[27]:


vtest_df = vassembler.transform(test_df)


# In[28]:


prediction = model.transform(vtest_df)


# In[29]:


prediction.show()


# ## Evaluation

# In[30]:


model.summary.rootMeanSquaredError 


# In[31]:


model.summary.r2


# - R2 : 70%가 나온것으로 봤을때, 좋은 성능은 아니지만 적당하다는 것을 알 수 있다.

# In[32]:


from pyspark.sql.types import DoubleType
distance_list = [1.1, 5.5, 10.5, 30.0]
distance_df = spark.createDataFrame(distance_list, DoubleType()).toDF('trip_distance')


# In[33]:


distance_df.show()


# In[34]:


vdistance_df = vassembler.transform(distance_df)


# In[35]:


vdistance_df.show()


# In[36]:


model.transform(vdistance_df).show()


# - 10마일 정도 가는데 41불으로 예측된다.
# - 거리가 커질수록 예측률도 커지는 것으로 알 수 있었다.

# In[ ]:




