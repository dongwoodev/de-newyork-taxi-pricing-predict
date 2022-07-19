#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession


# In[3]:


MAX_MEMORY = '5g'
spark = SparkSession.builder.appName('taxi-fare-prediction')                    .config('spark.executor.memory', MAX_MEMORY)                    .config('spark.driver.memory', MAX_MEMORY)                    .getOrCreate()


# # 데이터프레임 생성

# ### 데이터 저장
# - 추후 데이터를 가져오기 시간 단축을 위함

# In[4]:


data_dir = "/Users/dongwoo/new_york/data"


# In[6]:


# 데이터 불러오기
train_df = spark.read.parquet(f"{data_dir}/train/")
test_df = spark.read.parquet(f"{data_dir}/test/")


# In[8]:


# 하이퍼 파라미터 튜닝을 위해 10%의 데이터만 가져옵니다.
toy_df = train_df.sample(False, 0.1, seed=1)


# In[9]:


toy_df.printSchema()


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

# In[10]:


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


# In[11]:


stages


# ### Vector Assembler & StandardScaler
# > Numerical Data를 벡터화 합니다.
# 
# 
# `passenger_count` >> `passenger_count_vector` >> **`passenger_count_scaled`**

# In[12]:


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


# In[13]:


stages


# ### 두 벡터화 된 컬럼을 다시 벡터화로 합친다.
# 
# `passenger_count_scaled` + `passenger_count_scaled` >> **`feature_vector`**

# In[14]:


assembler_inputs = [c + "_onehot" for c in cat_feats] + [n + "_scaled" for n in num_feats]
assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="feature_vector")
stages += [assembler]


# In[15]:


stages


# ## Hyper-Parameter Tuning

# In[23]:


from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder # CV, 그리드 형식 테스트
from pyspark.ml.evaluation import RegressionEvaluator

lr = LinearRegression(
    maxIter=30,
    solver='normal',
    labelCol='total_amount',
    featuresCol='feature_vector'
)

cv_stages = stages + [lr] 

# 파이프라인에 기존 stages + 모델을 넣습니다.
cv_pipe = Pipeline(stages=cv_stages)


# In[25]:


param_grid = ParamGridBuilder()            .addGrid(lr.elasticNetParam, [0.1, 0.2, 0.3, 0.4, 0.5])            .addGrid(lr.regParam, [0.01, 0.02, 0.03, 0.04, 0.05])            .build()

# 여러가지의 값들을 테스트 한다.


# In[26]:


cross_val = CrossValidator(estimator=cv_pipe,
                          estimatorParamMaps=param_grid,
                          evaluator=RegressionEvaluator(labelCol="total_amount"),
                          numFolds=5 )
# numFolds : 데이터를 5개로 나눔


# In[27]:


cv_model = cross_val.fit(toy_df)


# In[28]:


alpha = cv_model.bestModel.stages[-1]._java_obj.getElasticNetParam() # 베스트 모델의 마지막 요소 추출


# In[29]:


reg_param = cv_model.bestModel.stages[-1]._java_obj.getRegParam() # 베스트 모델의 마지막 요소 추출


# ## Training

# In[36]:


from pyspark.ml import Pipeline

tf_stages = stages # Processing을 완료한 stages
pipe = Pipeline(stages=tf_stages) # 파이프라인을 구축

tf_fit = pipe.fit(train_df) # fit


# In[37]:


v_train_df = tf_fit.transform(train_df) # transform


# In[38]:


v_train_df.printSchema()


# # Regression Modeling

# In[39]:


from pyspark.ml.regression import LinearRegression


# In[40]:


lr = LinearRegression(
    maxIter=5, # 반복수
    solver="normal",
    labelCol="total_amount", # Label(Traget)
    featuresCol='feature_vector', # 전처리 후 > feature -> feature_vector
    # 추가 #
    elasticNetParam = alpha,
    regParam = reg_param,
    )


# In[41]:


model = lr.fit(v_train_df)


# In[42]:


vtest_df = tf_fit.transform(test_df) # test 데이터는 fit을 하지 않습니다. 


# In[43]:


prediction = model.transform(vtest_df)


# In[44]:


prediction.show()


# In[45]:


prediction.cache()


# In[46]:


prediction.select(["trip_distance", "day_of_week","total_amount","prediction"]).show()


# ## Evaluation

# In[47]:


model.summary.rootMeanSquaredError 


# In[48]:


model.summary.r2


# - R2 : 80%가 나온것으로 봤을때, 지난 성능보다 향상됬다고 볼 수 있다.

# # Model Save

# In[49]:


model_dir = f"{data_dir}/model"
model.save(model_dir)


# In[51]:


from pyspark.ml.regression import LinearRegressionModel

lr_model = LinearRegressionModel().load(model_dir)
prediction = lr_model.transform(vtest_df)
prediction.show()


# In[ ]:




