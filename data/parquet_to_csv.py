import pandas as pd
month = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10','11', '12']
for i in range(len(month)):
    df = pd.read_parquet(f'./trips/yellow_tripdata_2021-{month[i]}.parquet', engine='pyarrow')
    df.to_csv(f'./trip_csv/yellow_tripdata_2021-{month[i]}.csv')
