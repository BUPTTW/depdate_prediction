import gc
from bayes_opt import BayesianOptimization
from sklearn.cross_validation import cross_val_score, StratifiedKFold, StratifiedShuffleSplit
from sklearn.metrics import log_loss, matthews_corrcoef, roc_auc_score
from sklearn.preprocessing import MinMaxScaler
import contextlib
# 贝叶斯优化
def BayesianSearch(clf, params):
    num_iter = 25 # n_iter一般设置在25到50之间，指要执行多少个贝叶斯优化步骤，越多的步骤越有可能找到一个好的最大值
    init_points = 10 # init_points一般在10到20之间，指要执行多少个随机探索步骤，随机探索可以通过多样化勘探空间来提供帮助
    bayes = BayesianOptimization(clf, params)
    bayes.maximize(init_points=init_points, n_iter=num_iter)
    params = bayes.res['max']['max_val']
    print(params.max['params'])
    return params
def xgb_evaluate(max_depth,n_estimators,eta,reg_lambda,reg_alpha):
    param = {
        'objective': 'binary:logistic',
        'eval_metric': 'auc',
        'random_state': 90,
        'min_chilid_weight':1,
        'colsample_bytree':0.9,
        'subsample':0.9}
    # 贝叶斯优化器生成的超参数
    param['max_depth'] = int(max_depth)
    param['n_estimators'] = int(n_estimators)
    param['eta'] = float(eta)
    param['reg_lambda'] = float(reg_lambda)
    param['reg_alpha'] = float(reg_alpha)
    # 3折交叉验证
    val = cross_val_score(xgb.XGBClassifier(**param),x_train, y_train ,scoring='roc_auc', cv=3).mean()
    return val
# 调参范围
adj_params = {'max_depth': (2, 15),
             'n_estimators':(0,20),
             'eta':(0.01,0.2),
             'reg_lambda':(0.3,2),
             'reg_alpha':(0.3,2)}
# 调用贝叶斯优化
BayesianSearch(xgb_evaluate, adj_params)