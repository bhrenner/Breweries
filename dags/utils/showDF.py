import pandas as pd

df = pd.read_parquet('datalake/silver/breweries.parquet/process_date=20240725', engine='pyarrow')

print(df[['country','state']])