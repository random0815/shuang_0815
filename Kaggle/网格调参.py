
# coding: utf-8

# In[11]:


import pandas as pd
import gc
from sklearn.externals import joblib
from sklearn.cross_validation import train_test_split
from sklearn.metrics import accuracy_score
import numpy as np
from sklearn.model_selection import GridSearchCV
from sklearn import metrics
from xgboost import XGBClassifier


# In[12]:


def load_train(filePath,rows):
    df1 = pd.read_csv(filePath,nrows = rows)
    X = df1.drop(['is_attributed'],axis = 1)
    Y = df1['is_attributed']
    del df1
    gc.collect()
    return X,Y


# In[13]:


filePath = 'data/feature_train1kw.csv'
X,Y = load_train(filePath,10000000)
train_x, test_x ,train_y, test_y = train_test_split( X, Y, test_size=0.33, random_state=0)
del X,Y
gc.collect()


# In[15]:


cv_params = {'n_estimators': [50,100,150,200]}
other_params = {'learning_rate': 0.1, 'n_estimators': 100, 'max_depth': 6, 'min_child_weight': 10, 'seed': 0,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}
model = XGBClassifier(**other_params)


# In[9]:


cv_params = {'learning_rate': [0.05,0.08,0.1,0.2],'max_depth':[3,4,5,6,7]}
other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 6, 'min_child_weight': 10, 'seed': 0,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}
model = XGBClassifier(**other_params)


# In[10]:


optimized_GBM = GridSearchCV(estimator=model, param_grid=cv_params, scoring='roc_auc', cv=5, verbose=1, n_jobs=4)
optimized_GBM.fit(train_x, train_y)
evalute_result = optimized_GBM.grid_scores_
print('每轮迭代运行结果:{0}'.format(evalute_result))
print('参数的最佳取值：{0}'.format(optimized_GBM.best_params_)) 
print('最佳模型得分:{0}'.format(optimized_GBM.best_score_))


# In[16]:


optimized_GBM = GridSearchCV(estimator=model, param_grid=cv_params, scoring='roc_auc', cv=5, verbose=1, n_jobs=4)
optimized_GBM.fit(train_x, train_y)
evalute_result = optimized_GBM.grid_scores_
print('每轮迭代运行结果:{0}'.format(evalute_result))
print('参数的最佳取值：{0}'.format(optimized_GBM.best_params_))
print('最佳模型得分:{0}'.format(optimized_GBM.best_score_))


# In[ ]:


test_y, y_pred = test_y, clf.predict(test_x)
print("Accuracy : %.4g" % metrics.accuracy_score(test_y, y_pred))
y_proba=clf.predict_proba(test_x)[:,1]
print ("AUC Score (Train): %f" % metrics.roc_auc_score(test_y, y_proba)) 

