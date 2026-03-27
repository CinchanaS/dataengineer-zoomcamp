#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


year=2021
month=1


prefix="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
url=f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"
url


# In[6]:


df=pd.read_csv(url)


# In[6]:


df.head()


# In[7]:


len(df)


# In[8]:


df['VendorID']


# In[3]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates
)


# In[4]:


df.head()


# In[14]:


df


# In[15]:


get_ipython().system('uv add sqlalchemy')


# In[5]:

#can paramterize user, password, host and so ons
from sqlalchemy import create_engine
engine = create_engine('postgres://root:root@localhost:5432/ny_taxi')


# In[17]:


get_ipython().system('uv add psycopg2 --binary')


# In[6]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[20]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[21]:


#makes sure we only create the table, we don't add any data yet.
df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[7]:


len(df)


# In[27]:


df_obj = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000
)


# In[21]:


df1=next(df_obj)


# In[22]:


df1.head()


# In[23]:


for df in df_obj:
    print(len(df))


# In[14]:


#to see the ingestion progress
get_ipython().system('uv add tqdm')


# In[28]:


from tqdm.auto import tqdm


# In[26]:


print(df_obj)


# In[29]:


for df_chunks in tqdm(df_obj):
    print(len(df_chunks))
    df_chunks.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


# In[ ]:




