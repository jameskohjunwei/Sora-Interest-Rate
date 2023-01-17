import urllib.request
import pandas as pd
import json
from google.cloud import bigquery
import os
import gcloud
import sys
import datetime
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO

# set bq data env
os.environ["GCLOUD_PROJECT"] = 'insert your project name'
os.environ["GCLOUD_TABLE_ID"] = 'insert your table name'

client = bigquery.Client()

# sora query from mas api here
url = "https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=9a0bf149-308c-4bd2-832d-76c8e6cb47ed&limit=100&fields=end_of_day,comp_sora_1m&sort=end_of_day%5desc"

user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
headers={'User-Agent':user_agent,} 
request=urllib.request.Request(url,None,headers) #The assembled request
response = urllib.request.urlopen(request)
data = response.read() # The data u need
data = json.loads(data)

main_df = pd.DataFrame(columns=['date', 'one_month_sora', 'total_rate','loan_amt', 'interest_to_pay'])

rates_data = data['result']['records']

for rate in rates_data:
    # query fields of interest from json 
    one_month_sora = (rate['comp_sora_1m'])
    one_month_sora = float(0 if one_month_sora is None else one_month_sora)
    date = (rate['end_of_day'])
    timestamp = (rate['timestamp'])
    output = str(f"{date},{one_month_sora},{timestamp}")
    output = output.split(",")

    # print(output)
    main_df = main_df.append(pd.DataFrame([output], 
        columns=['date', 'one_month_sora', 'timestamp']))  

    # reinstate index
    main_df = main_df.reset_index()
    main_df = main_df.drop('index',axis=1)

    # convert date column to datetime
    main_df['date'] =  pd.to_datetime(main_df['date'], format='%Y-%m-%d')

    # include day of week
    main_df['day_of_week'] = main_df['date'].dt.day_name()

    # sort columns
    main_df = main_df[['date','day_of_week','one_month_sora','timestamp']]

    main_df["one_month_sora"].astype(float)

# upload results into bq
job = client.load_table_from_dataframe(main_df, 'datasetname')
job.result()
print(f"Uploading data to Google BigQuery is {job.state}")
