# -*- coding: utf-8 -*-
from config import DefaultConfig
import re
import os
import numpy as np
import gc
import pickle
import warnings
import xgboost as xgb
import pandas as pd
from pandas import DataFrame
import time
warnings.filterwarnings('ignore')

def load_model(model_file):
    with open(model_file, 'rb') as fin:
        model = pickle.load(fin)
    return model
def load_data(path, logger):
    data = pd.read_csv(path, sep='\t')
    logger.info('数据[%s]加载成功,[%s] '%(path,str(data.shape)))
    return data

# 数据预处理
def data_process(opt, logger):
    # ---------------------- 读取数据 ----------------------
    logger.info('用户出行时间预测数据预处理...')
    logger.info('1.数据读取开始')
    start = time.time()
    df = load_data(opt.predict_offline_data, logger)
    logger.info('数据读取完成，用时%f秒' % (time.time() - start))
    # ---------------------- 数据处理 ---------------------
    logger.info('2.缺失值处理')
    df = df.fillna(0)  # 将缺失值全部替换为0,主要是用户画像分类的缺失数据
    logger.info('数据预处理完成，用时%f秒' % (time.time() - start))
    return df

# ------------- 模型预测函数 ---------------
def predict(opt, logger, data):
    logger.info('用户出行时间预测:')
    logger.info('生成预测特征...')
    x_val = data.drop(['qunar_username', 'dep_date_of_search', 'search_date', 'dt'], axis=1)
    qunar_username = data.qunar_username
    dep_date_of_search = data.dep_date_of_search
    # ---------------------- 模型预测 ----------------------
    logger.info('3.开始预测')
    # 模型预测
    load_path = '{}/{}.model'.format(opt.model_dir, opt.model_pkl)
    model = load_model(load_path)
    val = xgb.DMatrix(x_val)
    pred_xgb = model.predict(val, ntree_limit=1500)
    res = pd.DataFrame({'qunar_username': qunar_username, 'dep_date': dep_date_of_search, 'q_ratio': pred_xgb})
    res[['qunar_username', 'dep_date', 'q_ratio']].to_csv(opt.result_filename, index=False, header=True)
    logger.info('模型预测完毕.')
    gc.collect()

if __name__ == '__main__':
    start = time.time()
    opt = DefaultConfig()
    logger = opt.logger
    data = data_process(opt, logger)
    #print(data.head(10))
    predict(opt,logger,data)
    logger.info('总共耗时%f秒' % (time.time() - start))