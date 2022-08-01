from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2022, 1, 1),
}

with DAG(dag_id='taxi-price-pipeline',
         schedule_interval='@daily',
         default_args = default_args,
         tags=['spark'],
         catchup=False) as dags:
    
    # Preprocessing
    preprocess = SparkSubmitOperator(
        application='/Users/dongwoo/new_york/batch/py/preprocess.py',
        task_id = "preprocess",
        conn_id="spark_local"
    )
    
    # Tuning Hyper Parameter
    tune_hyperparmeter = SparkSubmitOperator(
        application='/Users/dongwoo/new_york/batch/py/tune_hyperparameter.py',
        task_id = "tune_hyperparmeter",
        conn_id="spark_local"
    )
    
    # training Model
    train_model = SparkSubmitOperator(
        application='/Users/dongwoo/new_york/batch/py/train_model.py',
        task_id = "train_model",
        conn_id="spark_local"
    )
    
    # 의존성 
    preprocess >> tune_hyperparmeter >> train_model