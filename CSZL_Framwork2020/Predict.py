#coding=utf-8

#import pandas as pd
#import numpy as np

import Display
import FeatureEngineering as FE

import Dataget
import Models
import FileIO

import datetime
import time


class Predict(object):
    """实时预测输出"""

    def TodayPredict5Day_0606(this):
        REAL_Get=Dataget.Dataget()
        datepath,adjpath,longpath,moneyflowpath=REAL_Get.get_history_dateset_four()
        #datepath,adjpath,moneyflowpath=REAL_Get.get_history_dateset()

        REAL_Get.real_get_change(datepath)
        REAL_Get.real_get_adj_change(adjpath)
        REAL_Get.real_get_long_change(longpath)

        REAL_Get.real_get_moneyflow_change(moneyflowpath)

        #选择特征工程
        #切换为e
        #cur_fe=FE.FEg30eom0110onlinef()
        cur_fe=FE.FEonlinew_a31()   
        cur_fe.real_FE()

        #
        nowTime=datetime.datetime.now()
        month_sec=nowTime.strftime('%Y%m%d')  
        print(month_sec)
        #选择模型
        cur_model=Models.LGBmodel_20()
        cur_model.real_lgb_predict('lgb16.pkl','out1.csv')
        cur_model.real_lgb_predict('lgb26.pkl','out2.csv')
        cur_model.real_lgb_predict('lgb36.pkl','out3.csv')
        cur_model.real_lgb_predict('lgb46.pkl','out4.csv')

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

    def PredictBackRound(self):

        cur_date=datetime.datetime.now().strftime("%Y-%m-%d")
        change_flag=0
        while(True):
            date=datetime.datetime.now()
            day = date.weekday()
            if(day>4):
                time.sleep(10000)
                continue
                dawd=5
            if(day==4):
                cur_inputflag=1
            else:
                cur_inputflag=0

            if(self.CSZL_TimeCheck()):       

                if(cur_inputflag==1):
                    self.TodayPredict5Day_0606()
                else:
                    self.TodayPredict5Day_0606()

                time.sleep(10000) 
        

            print(date)
            time.sleep(10)

    def CSZL_TimeCheck(self):
        global CurHour
        global CurMinute



        CurHour=int(time.strftime("%H", time.localtime()))
        CurMinute=int(time.strftime("%M", time.localtime()))

        caltemp=CurHour*100+CurMinute

        #return True

        if (caltemp>=1452 and caltemp<=1500):
            return True
        else:
            return False 