from  config import DefaultConfig
import pandas as pd
import numpy as np
import warnings
from sklearn import model_selection, metrics
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score, roc_auc_score
import xgboost as xgb
import pickle
import gc
import time
warnings.filterwarnings('ignore')


# 模型参数设置
def model_train(opt, train_x, train_y, val_x, val_y):
    xgb_train = xgb.DMatrix(train_x, train_y, silent=True)
    xgb_val = xgb.DMatrix(val_x, val_y, silent=True)
    params_xgb = {
        'booster': opt.booster,
        'objective': opt.objective,
        'gamma': opt.gamma,
        'max_depth': opt.max_depth,  # 树最大深度
        'lambda': opt.lambda_xgb,  # L2
        'alpha': opt.alpha,  # L1
        'subsample': opt.subsample,
        'colsample_bytree': opt.colsample_bytree,  # 在建立树时对特征采样的比例。缺省值为1
        'colsample_bylevel': opt.colsample_bylevel,
        'eval_metric': opt.eval_metric,
        'min_child_weight': opt.min_child_weight,
        'max_delta_step': opt.max_delta_step,
        'silent': opt.silent,  # 当这个参数值为1时，静默模式开启，不会输出任何信息。一般这个参数就保持默认的0，因为这样能帮我们更好地理解模型。
        'eta': opt.eta,  # 权重衰减因子eta为0.01~0.2
        'seed': opt.seed,  # 随机数的种子。缺省值为0。
        'scale_pos_weight': opt.scale_pos_weight,
        'tree_method': opt.tree_method,
        'nthread': opt.nthread,
        'early_stopping_rounds': opt.early_stopping_rounds}
    watchlist = [(xgb_train, 'train'), (xgb_val, 'val')]
    num_boost_round = opt.num_boost_round  # 修改至2000
    plst = params_xgb.items()
    model_xgb = xgb.train(plst, xgb_train, num_boost_round, evals=watchlist, verbose_eval=100, maximize=1)
    return model_xgb


# 加载模型
def load_model(model_file):
    with open(model_file, 'rb') as fin:
        model = pickle.load(fin)
    return model


# 模型验证
def model_validate(X_val, y_val, model, threshold):
    val = xgb.DMatrix(X_val)
    print('model.best_iteration:', model.best_iteration)
    # pred_xgb_1 = model.predict(val,ntree_limit = model.best_iteration)
    pred_xgb_1 = model.predict(val, ntree_limit=1500)
    y_pred_1 = [1 if i > threshold else 0 for i in pred_xgb_1]

    print('预测结果集：', len(y_pred_1))
    print('阈值>%s 为正样本' % threshold)
    print(classification_report(y_val, y_pred_1))
    print('Accracy:', accuracy_score(y_val, y_pred_1))
    print('AUC: %.4f' % metrics.roc_auc_score(y_val, y_pred_1))
    print('ACC: %.4f' % metrics.accuracy_score(y_val, y_pred_1))
    print('Accuracy: %.4f' % metrics.accuracy_score(y_val, y_pred_1))
    print('Recall: %.4f' % metrics.recall_score(y_val, y_pred_1))
    print('F1-score: %.4f' % metrics.f1_score(y_val, y_pred_1))
    print('Precesion: %.4f' % metrics.precision_score(y_val, y_pred_1))

    return pred_xgb_1


def load_data(path, logger):
    data = pd.read_csv(path, sep='\t')
    logger.info('数据[%s]加载成功,[%s] '%(path,str(data.shape)))
    return data


def save_model(model, save_path):
    with open(save_path, 'wb') as fout:
        pickle.dump(model, fout)
    print('训练模型保存至：', str(save_path))


def data_process(opt, logger):
    logger.info('出行时间预测数据预处理...')
    logger.info('1.数据读取开始')
    start = time.time()
    df = load_data(opt.train_offline_data, logger)
    logger.info('数据读取完成，用时%f秒' % (time.time() - start))

    logger.info('2.数据预处理...')
    # 数据预处理
    logger.info('缺失值处理')
    df.fillna(0) #主要是用户画像分类的缺失数据
    logger.info('数据预处理完成，用时%f秒' % (time.time() - start))
    return df


def train(opt, logger, data):
    logger.info('出行时间预测模型训练:')
    # ---------------------- 样本拆分 ----------------------
    # 切分正负样本
    df_pos = data[data.label == 1].sample(n=opt.n_sample, random_state=1)
    df_neg = data[data.label == 0].sample(n=opt.n_sample, random_state=43)
    ## 验证集
    df_val = data.drop(index=df_pos.index)
    df_val = df_val.drop(index=df_neg.index)
    print('正样本量：%d,负样本量:%d, 测试集量:%d' % (len(df_pos), len(df_neg), len(df_val)))
    # 合并正负样本
    dfv1 = df_pos.append(df_neg)

    df_x = dfv1.drop(['qunar_username', 'dep_date_of_search', 'search_date', 'pre_days_median', 'label'], axis=1)
    df_y = dfv1['label']
    print('df_x的数据集大小：\n', df_x.shape)
    print('df_y的数据集大小：\n', df_y.shape)

    # ---------------------- 模型训练 ----------------------

    # 提取训练数据
    x_train, x_test, y_train, y_test = train_test_split(df_x, df_y, test_size=0.5, random_state=43)
    print('模型训练...')
    model = model_train(opt, x_train, y_train, x_test, y_test)
    print('模型训练结束...')

    # ---------------------- 模型评估 -------------------------
    threshold = 0.5
    model_name = model

    try:
        model
    except NameError:
        model = load_model(model_name)

    valset = df_val
    X_val = valset.drop(['qunar_username', 'dep_date_of_search', 'search_date', 'pre_days_median', 'label'], axis=1)
    y_val = valset['label']

    print('模型在测试集上的效果：')
    print('threshold = ', threshold)
    pred = model_validate(X_val, y_val, model, threshold=threshold)
    y_pred = [1 if i >= threshold else 0 for i in pred]

    print('混淆矩阵:')
    df_val = pd.DataFrame()
    df_val['true_label'] = y_val
    df_val['prediction_label'] = y_pred
    df_val['id'] = 1
    pivot_df = pd.crosstab(index=df_val.prediction_label,
                           columns=df_val.true_label,
                           values=df_val.id,
                           aggfunc='count')
    print(pivot_df)
    gc.collect()
    logger.info('模型训练完毕!')

    # ---------------------- 保存模型 ----------------------
    save_model(model, '../model/udp_model.sample')
    logger.info('出行日期预测模型训练完毕,模型保存OK')
    gc.collect()

if __name__ == '__main__':
    opt = DefaultConfig()
    logger = opt.logger
    data = data_process(opt, logger)
    train(opt, logger, data)