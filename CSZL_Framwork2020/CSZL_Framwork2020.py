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
# 2/3 -20607
# 4/16 -7621
# 5/1 -795

#两个主要功能 实时预测 和 回测
#其他功能 基金选择 参数寻找


if __name__ == '__main__':

    ##读取token
    #f = open('token.txt')
    #token = f.read()     #将txt文件的所有内容读入到字符串str中
    #f.close()

    #pro = ts.pro_api(token)

    #df = pro.index_daily(ts_code='000001.SH', start_date='20130105', end_date='20200105')
    #print(df)
    #df.to_csv('./szzs.csv',encoding = 'gbk')

    #date=pro.query('trade_cal', start_date='20130105', end_date='20200105')
    #date=date[date["is_open"]==1]
    #bufferlist=date["cal_date"]
    #get_list=bufferlist.values

    #df_all=pro.share_float(ann_date='20130104')
    #for day in get_list:      
    #    try:
    #        df = pro.share_float(ann_date=day)
    #        df_all=pd.concat([df_all,df])
    #        time.sleep(1)
    #        print(day)

    #    except Exception as e:
    #        df_all.to_csv('./float2jiejing.csv')

    #df_all.to_csv('./float2jiejing.csv',encoding = 'gbk')

    #print(df)

    #df_all.to_csv('./Daily2rong.csv')
    ##获取单日全部股票数据涨跌停价格
    ##df = pro.stk_limit(trade_date='20120409')


    ###获取单个股票数据
    ##df = pro.stk_limit(ts_code='000018.SZ', start_date='20191115', end_date='20191230')
    ##print(df)
    #zzz=Predict.Predict()
    ###zzz.PredictBackRound()
    #zzz.TodayPredict5Day_0517()

    #dis=Display.Display()
    #dis.plotall_test()


    bt=BT.BackTesting()

    bt.backTesting()


    #bt.backtesting_forpara()

    bt.backTestingWithPredictDatas()

    bt.backTesting()