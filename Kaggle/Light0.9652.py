import pandas as pd
import gc
import xgboost as xgb
from sklearn.externals import joblib
from sklearn.cross_validation import train_test_split
from sklearn.metrics import accuracy_score
import lightgbm as lgb
from sklearn.metrics import roc_auc_score, recall_score, f1_score,precision_score
from sklearn.externals import joblib

df1 = pd.read_csv('data/0416_train.csv')

predictors = ['app', 'device','os', 'channel', 'hour','day',
             'ip_app_channel_day_count','ip_app_channel_mean_hour','ip_app_count','ip_count_all',
             'ip_day_count']
categorical = ['app', 'device', 'os', 'channel', 'hour', 'day']

target = 'is_attributed'
# X = df1.drop([target],axis = 1)
# Y = df1[target]
X = df1[predictors]
Y = df1[target]
train_x, test_x ,train_y, test_y = train_test_split(X, Y, test_size=0.1, random_state=99)

dtrain = lgb.Dataset(train_x.values, label=train_y.values,feature_name=predictors,
                      categorical_feature=categorical)
dvalid = lgb.Dataset(test_x.values, label=test_y.values,feature_name=predictors,
                      categorical_feature=categorical)
params = {

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

evals_results = {}
lgb_model = lgb.train(params,
                 dtrain,
                 valid_sets=[dtrain, dvalid],
                 valid_names=['train','valid'],
                      evals_result=evals_results,
                      num_boost_round=1000,
                      early_stopping_rounds=30,
                      verbose_eval=10,
                      feval=None)

# Feature names:
print('Feature names:', lgb_model.feature_name())

# Feature importances:
print('Feature importances:', list(lgb_model.feature_importance()))
#模型预测部分
##读取测试集
fileinputPath = 'data/0416_test.csv '#测试集文件路径
submit = pd.read_csv(fileinputPath)
index = submit[['click_id']]#choose index
index['click_id'] = index['click_id'].astype('uint32')
print("Predicting the submission data...")

index['is_attributed']  = lgb_model.predict(submit[predictors], num_iteration=lgb_model.best_iteration)

submitFile = 'data/submit4.22_2.csv'
print("Writing the submission data into a csv file...")

index.to_csv(submitFile, index=False)

# predictors = ['app', 'device','os', 'channel', 'hour',
#              'ip_app_channel_day_count','ip_app_count','ip_count_all',
#              'ip_day_count']
# categorical = ['app', 'device', 'os', 'channel', 'hour']
# # 0.9653