#coding=utf-8

import pandas as pd
import numpy as np

import Display
import FeatureEnvironment as FE

import Dataget
import Models
import FileIO

import datetime



class Predict(object):
    """description of class"""
    def TodayPredict5Day(this):
        REAL_Get=Dataget.Dataget()
        datepath,adjpath=REAL_Get.get_history_dateset()

        REAL_Get.real_get_change(datepath)
        REAL_Get.real_get_adj_change(adjpath)

        #选择特征工程
        cur_fe=FE.FEg30b()    
        cur_fe.real_FE()

        #
        nowTime=datetime.datetime.now()
        month_sec=nowTime.strftime('%Y%m%d')  

        #选择模型
        cur_model=Models.LGBmodel()
        cur_model.real_lgb_predict('lgb1.pkl','out1.csv')
        cur_model.real_lgb_predict('lgb2.pkl','out2.csv')
        cur_model.real_lgb_predict('lgb3.pkl','out3.csv')
        cur_model.real_lgb_predict('lgb4.pkl','out4.csv')

        #展示类
        dis=Display.Display()
        dis.show_today()

        todaypath='./result'+month_sec
        FileIO.FileIO.mkdir(todaypath)

        FileIO.FileIO.copyfile('out1.csv',todaypath+'/out1.csv')
        FileIO.FileIO.copyfile('out2.csv',todaypath+'/out2.csv')
        FileIO.FileIO.copyfile('out3.csv',todaypath+'/out3.csv')
        FileIO.FileIO.copyfile('out4.csv',todaypath+'/out4.csv')


        asdad=1

