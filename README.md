# 🚕 NewYork Taxi Pricing Predict
201637011 강동우

# 1. 배경
## 의사 결정 배경
과거 기반의 데이터와 실시간 데이터를 기반으로 수요와 공급의 예측을 통해 가격을 조정하려고 한다.

## 파일 트리
```

```
## 기술 스택
- Python 3.8
- Spark SQL, ML
- Airflow
- Flink
- Kafka

## 파이프 라인

- Batch Processing(과거 기반) :  머신러닝 학습
- Stream Processing(실시간) : 걸리는 시간 예측

<img src="./templates/readme_pipeline.png" width="800">

## 실행 계획
|No.|Title|Decription|Stack|URL|
|---|---|---|---|---|
|1|New York Taxi Data Analysis (뉴욕 택시 데이터 분석)|뉴욕 택시 데이터 분석을 진행|`spark SQL`, `jupyter Notebook`||
|2|Taxi Pricing Prediction (택시비  예측)|택시비 예측 후 파라미터 최적화 및 모델 저장|`spark ML`, `jupyter Notebook`||
|3|Taxi Pricing PipeLine (택시비 예측 파이프라인 관리) |Airflow를 통해 택시비에 대한 파이프라인을 구축및 관리|`Airflow`, `Spark`||
|4|Taxi Pricing Event Processing (택시비 이벤트 처리) |카프카를 이용하여 택시 Producer, Topic을 만들고 메세지 확인할 수 있게 구현|`Kafka`, `Spark`||
|5|Taxi Pricing (택시정보 받아 택시비 예측) |Flink를 이용하여 택시 정보를 받아 택시비 예측|`Flink`, `Spark`||

# 2. 프로세스 노트
1. TLC 사이트에서 데이터를 수집 후 Spark SQL 쿼리를 통해 데이터 프레임 생성 
2. Outlier 제거를 위한 데이터 클리닝
3. 시각화 작업 
    - `pickup_date`를 활용한 택시 이용률
    - `DATE_FORMAT` 이나 사용자 정의 함수를 이용해 요일별 택시 이용률
    - 사용자 정의 함수를 이용해 지불 타입 도출

## 분석 및 시각화
<img src="./templates/날짜별_택시_이용_시각화.png" width="500">
<img src="./templates/요일별_택시_이용_시각화.png" width="400">

- 우선, 택시 이용자 수가 증가하는 것을 알 수 있으며, 요일 마다 격차가 크다는 것을 알 수 있었다.
- 요일 별로 보게되면, 다른 요일에 비해 월요일 택시 이용자 수가 적다는 것을 알 수 있다.

<img src="./templates/택시 이용에 따른 지불 방법_시각화.png" width="400">

- 지불 방식에 대해서는 Credit Card로 지불하는 사람들이 가장 많으며, 금액에 대한 분쟁도 꽤 일어난다는 것을 알 수 있다.

## 머신러닝 예측
|No|r2 score|RMSE|Description|Link|
|---|---|---|---|---|
|1|0.70|7.91|초기 예측 모델|[💾]()|
|2|0.81|6.2|- OneHot Encoding, Standard Scaling, Vector Assembler등 전처리|[💾]()|

- 10마일 정도 가는데 41불 정도로 예측되었고 거리가 길수록 예측 확률도 높아진다는 것을 알 수 있었다. (초기 예측)
- 전처리 이후, 좋은 성능이 나온 것으로 확인되었다.
## 성능 개선
- 데이터 클리닝 도중, `total_amount` 와 `trip_distance`등 에서 이상값(Outlier)를 발견
    > 해결 : `WHERE` 을 통해 새 쿼리를 만들어 데이터 전처리
- 초기 예측 성능이 70%(R2)정도 나왔다. 좋은 성능은 아니지만 적당한 결과였다. 
- `OneHoTEncoding`과 `StandardScaler`, `VectorAssembler`를 통해 numerical Data와 Categorical data를 전처리 한 컬럼을 이용해 예측 성능을 내보았다.
    > 해결 : 지난 초기 예측 성능 보다 10% 향상된 좋은 성능이 나왔다. (80%)

