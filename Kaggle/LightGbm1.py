import pandas as pd
import gc
import xgboost as xgb
from sklearn.externals import joblib
from sklearn.model_selection import train_test_split,GridSearchCV
from sklearn.metrics import accuracy_score
import lightgbm as lgb
from sklearn.metrics import roc_auc_score, recall_score, f1_score,precision_score
from sklearn.externals import joblib
from sklearn.model_selection import ParameterGrid
import numpy as np
import time

df1 = pd.read_csv('data/0416_train.csv')

predictors = ['app', 'device','os', 'channel', 'hour',
            'ip_app_channel_day_count','ip_app_count','ip_count_all',
              'ip_day_count']
categorical = ['app', 'device', 'os', 'channel', 'hour']

target = 'is_attributed'
X = df1[predictors]
Y = df1[target]
train_x, test_x ,train_y, test_y = train_test_split(X, Y, test_size=0.1,stratify=Y, random_state=99)
train_sum=train_y.sum()
# print(train_y.size)
weight_dict=dict()
weight_dict[0]=train_y.size/(train_y.size-train_sum)
weight_dict[1]=train_y.size/train_sum
train_weight=np.array(train_y.apply(lambda x:weight_dict[x]))
# print(train_weight[:10])
dtrain = lgb.Dataset(train_x.values, label=train_y.values,feature_name=predictors,weight=train_weight,
                      categorical_feature=categorical,free_raw_data=False)
test_sum=test_y.sum()
weight_dict=dict()
weight_dict[0]=test_y.size/(test_y.size-test_sum)
weight_dict[1]=test_y.size/test_y
test_weight=np.array(test_y.apply(lambda x:weight_dict[x]))
# print(test_weight[:10])
dvalid = lgb.Dataset(test_x.values, label=test_y.values,feature_name=predictors,weight=test_weight,
                      categorical_feature=categorical)
default_params = {
    'boosting_type': 'gbdt',
    'objective': 'binary',
    'metric': 'auc',
    'learning_rate': 0.1,
    #'is_unbalance': 'true', # replaced with scale_pos_weight argument
    'num_leaves': 7,  # 2^max_depth - 1
    'max_depth': 3,  # -1 means no limit
    'min_child_samples': 300,  # Minimum number of data need in a child(min_data_in_leaf)
    'max_bin': 150,  # Number of bucketed bin for feature values
    'subsample': 0.9,  # Subsample ratio of the training instance.
    'subsample_freq': 1,  # frequence of subsample, <=0 means no enable
    'colsample_bytree': 0.9,  # Subsample ratio of columns when constructing each tree.
    'min_child_weight': 0,  # Minimum sum of instance weight(hessian) needed in a child(leaf)
    'scale_pos_weight':400, # because training data is extremely unbalanced
    'subsample_for_bin': 200000,  # Number of samples for constructing bin
    'min_split_gain': 0,  # lambda_l1, lambda_l2 and min_gain_to_split to regularization
    'reg_alpha': 0.99,  # L1 regularization term on weights
    'reg_lambda': 0.9,  # L2 regularization term on weights
}

#所调参数
search_param = {
    'max_depth': [i for i in range(3,8)],
    'learning_rate':[i/100 for i in range(7,10)],
    'subsample' :[i/10 for i in range(7,9)]
}

tot_result = {}

def dict_to_str(m):

    string=None
    for k,v in m.items():
        if string is None:
            string=""
        else:
            string=string+","
        string=string+str(k)+":"+str(v)
    return string

for add_param in ParameterGrid(search_param):
    params=dict(default_params,**add_param)
    print('now fit param:'+str(add_param))
    print(dict_to_str(add_param))
    start_time=time.time()
    result=lgb.cv(params,dtrain,nfold=5,verbose_eval=10,num_boost_round=1000,early_stopping_rounds=30,metrics=['auc'])
    tot_result[dict_to_str(add_param)]=result
    print(result)
    print('score:'+str(max(result['auc-mean'])))
    end_time=time.time()
    print('花费时间%d s'%(end_time-start_time))
best_score=0
best_param=None
for k,v in tot_result.items():
    print(str(k))
    score=max(v['auc-mean'])
    if score>best_score:
        best_param=k
        best_score=score
    for m,n in v.items():
        print('\t %s:'%m,str(n))
print('best_param : best_score')
print(str(best_param)+" : "+str(best_score))


# evals_results = {}
# lgb_model = lgb.train(default_params,
#                  dtrain,
#                  valid_sets=[dtrain, dvalid],
#                  valid_names=['train','valid'],
#                  evals_result=evals_results,
#                  num_boost_round=350,
#                  early_stopping_rounds=30,
#                  verbose_eval=True,
#                  feval=None)
# lgb_model.save_model('lgb.model')
# param1={'learning_rate':[i/100 for i in range(1,21)]}
# cv1=GridSearchCV(estimator=lgb_model,param_grid=param1,n_jobs=3,cv=5)
#
# # Feature names:
# print('Feature names:', lgb_model.feature_name())
#
# # Feature importances:
# print('Feature importances:', list(lgb_model.feature_importance()))
# #模型预测部分
# ##读取测试集
# fileinputPath = 'data/0416_test.csv '#测试集文件路径
# submit = pd.read_csv(fileinputPath)
# index = submit[['click_id']]#choose index
# index['click_id'] = index['click_id'].astype('uint32')
# print("Predicting the submission data...")
#
# index['is_attributed']  = lgb_model.predict(submit[predictors], num_iteration=lgb_model.best_iteration)
#
# submitFile = 'data/submit4.16_3.csv'
# print("Writing the submission data into a csv file...")
#
# index.to_csv(submitFile, index=False)