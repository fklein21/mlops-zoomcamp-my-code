#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip freeze | grep scikit-learn')


# In[2]:


import pickle
from pathlib import Path
import pandas as pd


# In[13]:


year = 2021
month = 2

input_file = f'https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_{year:04d}-{month:02d}.parquet'
output_file = f'output/fhv_tripdata_{year:04d}-{month:02d}_prediction.parquet'
Path(output_file).resolve().parent.mkdir(parents=True, exist_ok=True)


# In[14]:


with open('model.bin', 'rb') as f_in:
    dv, lr = pickle.load(f_in)


# In[15]:


categorical = ['PUlocationID', 'DOlocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df


# In[16]:


df = read_data(input_file)


# In[17]:


dicts = df[categorical].to_dict(orient='records')
X_val = dv.transform(dicts)
y_pred = lr.predict(X_val)


# In[18]:


y_pred.mean()


# In[19]:


df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')


# In[20]:


df_result = pd.DataFrame()
df_result['ride_id'] = df['ride_id']
df_result['prediction'] = y_pred


# In[21]:


df_result.to_parquet(
    output_file,
    engine='pyarrow',
    compression=None,
    index=False
)


# In[22]:


get_ipython().system('ls -lha output')


# In[ ]:




