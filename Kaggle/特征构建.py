
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import gc


# In[2]:


def change_time(data=None,input_col='click_time',output_cols=None,drop_input=True):
    data['time']=pd.to_datetime(data[input_col])
    if 'day' in output_cols:
        data['day']=data['time'].dt.day
    if 'hour' in output_cols:
        data['hour']=data['time'].dt.hour
    if 'minute'in output_cols:
        data['minute']=data['time'].dt.minute
    # if 'second' in output_cols:
    #     data['second']=data['time'].dt.second
    # if 'dayweek' in output_cols:
    #     data['dayweek']=data['time'].dt.dayofweek
    if (drop_input):
        return data.drop(['time',input_col],axis=1)
    else:
        return data.drop('time',axis=1)


# In[3]:


def role_time_feature(data = None, input_col = "ip"):
    print(input_col + "正在构造特征")
    df = data.groupby(input_col,as_index=False)[input_col].agg({input_col + "_count_all":np.size})
    data = data.merge(df,on = input_col, how='left')
    del df
    gc.collect()
    print(str([input_col,"day"]) + "正在构造特征")
    df = data.groupby([input_col,"day"],as_index=False)[input_col].agg({input_col + "_count_day":np.size})
    data = data.merge(df, on=[input_col,"day"], how='left')
    del df
    gc.collect()
    print(str([input_col,"day","hour"]) + "正在构造特征")
    df = data.groupby([input_col,"day","hour"],as_index=False)[input_col].agg({input_col + "_count_hour":np.size})
    data = data.merge(df, on=[input_col,"day","hour"], how='left')
    del df
    gc.collect()
    print(str([input_col,"day","hour","minute"]) + "正在构造特征")
    df = data.groupby([input_col,"day","hour","minute"],as_index=False)[input_col].agg({input_col + "_count_minute":np.size})
    data = data.merge(df, on=[input_col,"day","hour","minute"], how='left')
    del df
    gc.collect()
    
    return data


# In[4]:


# 修改数据输入和输出的路径
train_input_path='data/train.csv'
train_output_path = "data/feature_train1kw.csv"
data = pd.read_csv(train_input_path, skiprows=range(1,144903891), nrows=10000000, usecols=['ip','app','device','os', 'channel', 'click_time', 'is_attributed'])
data = change_time(data,output_cols = ["hour", "minute","day"])
gc.collect()
data = role_time_feature(data,"ip")
data = role_time_feature(data,"app")
data.to_csv(train_output_path,index = False)


# In[5]:


data.head(10)

