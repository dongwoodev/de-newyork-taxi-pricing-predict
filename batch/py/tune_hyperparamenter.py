from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, StandardScaler # Preprocessing
from pyspark.ml.regression import LinearRegression 
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder # CV, Grid
from pyspark.ml.evaluation import RegressionEvaluator 
import numpy as np
import pandas as pd

MAX_MEMORY = '5g'
MAX_MEMORY = '5g'
spark = SparkSession.builder.appName('taxi-fare-prediction')\
                .config('spark.executor.memory', MAX_MEMORY)\
                .config('spark.driver.memory', MAX_MEMORY)\
                .getOrCreate()
                
# Importing Data
data_dir = "/Users/dongwoo/new_york/data"
train_df = spark.read.parquet(f"{data_dir}/train/")
test_df = spark.read.parquet(f"{data_dir}/test/")

# Importing a little data for tuning
toy_df = train_df.sample(False, 0.1, seed=1)

# Categorical Data
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

# Numerical Data
num_feats = [
    "passenger_count",
    "trip_distance",
    "pickup_time"
]

for n in num_feats:
    num_assembler = VectorAssembler(inputCols=[n], outputCol= n + "_vector")
    num_scaler = StandardScaler(inputCol=num_assembler.getOutputCol(), outputCol= n + "_scaled")
    
    stages += [num_assembler, num_scaler]

# Merge
assembler_inputs = [c + "_onehot" for c in cat_feats] + [n + "_scaled" for n in num_feats]
assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="feature_vector")
stages += [assembler]

# Modeling

lr = LinearRegression(
    maxIter=30,
    solver='normal',
    labelCol='total_amount',
    featuresCol='feature_vector'
)

cv_stages = stages + [lr] 

cv_pipe = Pipeline(stages=cv_stages)
param_grid = ParamGridBuilder()\
                .addGrid(lr.elasticNetParam, [0.1, 0.2, 0.3, 0.4, 0.5])\
                .addGrid(lr.regParam, [0.01, 0.02, 0.03, 0.04, 0.05])\
                .build()

cross_val = CrossValidator(estimator=cv_pipe,
                          estimatorParamMaps=param_grid,
                          evaluator=RegressionEvaluator(labelCol="total_amount"),
                          numFolds=5) # CV를 5개로 나눠 하나씩 실험

cv_model = cross_val.fit(toy_df)
alpha = cv_model.bestModel.stages[-1]._java_obj.getElasticNetParam() # 베스트 모델의 마지막 요소 추출
reg_param = cv_model.bestModel.stages[-1]._java_obj.getRegParam() # 베스트 모델의 마지막 요소 추출

hyperparam = {
    'alpha' : [alpha],
    'reg_param' : [reg_param]
}

# Hyperparamer 값 CSV 파일로 저장

hyper_df = pd.DataFrame(hyperparam).to_csv(f"{data_dir}hyperparameter.csv")
print(hyper_df)



