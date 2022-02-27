import os
from google.cloud import bigquery

from config import DB_TABLELIST_DICT


# Download Data from GCP BigQuery
# authentication, json is secret key created that has been downloaded to my local server
credential_path = r'C:\Users\haneu\Desktop\PROJECTS\Multi-cloud\credentials\gcp\festive-folio-342512-5321f01c1499.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

client = bigquery.Client()
q = """
    SELECT *
      FROM tmdb_credits.ratings
"""
project_id = 'festive-folio-342512'
df = client.query(q, project=project_id).to_dataframe()
df.to_parquet("data/tmdb_ratins.parquet")

# 각 DB의 테이블들을 전부 한번의 코드로 옮기기 위함.
for db,table_lst in DB_TABLELIST_DICT.items():
    for table in table_lst:
        q = f"""SELECT * FROM {db}.{table}"""
        df = client.query(q, project=project_id).to_dataframe()
        # save to local
        df.to_parquet(f"data/{db}/{table}.parquet")


