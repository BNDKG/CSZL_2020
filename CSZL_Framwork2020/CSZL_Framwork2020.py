#coding=utf-8

import pandas as pd
import numpy as np

import Display
import FeatureEnvironment as FE

import Dataget
import BackTesting as BT
import Models
import FileIO
import Predict

import tushare as ts
import datetime
import sys
import os
import shutil
import datetime
import time
import random

# 2020年 目标20w
# 1/12 +3891
# 1/26 -5913


#两个主要功能 实时预测 和 回测
#其他功能 基金选择 参数寻找


if __name__ == '__main__':

    ##读取token
    #f = open('token.txt')
    #token = f.read()     #将txt文件的所有内容读入到字符串str中
    #f.close()

    #pro = ts.pro_api(token)

    ##获取单日全部股票数据涨跌停价格
    ##df = pro.stk_limit(trade_date='20120409')


    ##获取单个股票数据
    #df = pro.stk_limit(ts_code='000018.SZ', start_date='20191115', end_date='20191230')
    #print(df)
    zzz=Predict.Predict()
    #zzz.PredictBackRound()
    zzz.TodayPredict5Day()

    bt=BT.BackTesting()

    bt.backTesting()


    bt.backtesting_forpara()

    bt.backTestingWithPredictDatas()

    bt.backTesting()