#coding=utf-8

import pandas as pd
import numpy as np

import Display
import FeatureEnvironment as FE

import Dataget
import Models
import FileIO

import datetime
import time


class Predict(object):
    """description of class"""
    def TodayPredict5Day(this):
        REAL_Get=Dataget.Dataget()
        datepath,adjpath=REAL_Get.get_history_dateset_two()
        #datepath,adjpath,moneyflowpath=REAL_Get.get_history_dateset()

        REAL_Get.real_get_change(datepath)
        REAL_Get.real_get_adj_change(adjpath)
        #REAL_Get.real_get_long_change(longpath)

        #REAL_Get.real_get_moneyflow_change(moneyflowpath)

        #选择特征工程
        #切换为e
        cur_fe=FE.FEg30e()    
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

    def TodayPredict5Day_0517(this):
        REAL_Get=Dataget.Dataget()
        datepath,adjpath,longpath=REAL_Get.get_history_dateset_three()
        #datepath,adjpath,moneyflowpath=REAL_Get.get_history_dateset()

        REAL_Get.real_get_change(datepath)
        REAL_Get.real_get_adj_change(adjpath)
        REAL_Get.real_get_long_change(longpath)

        #REAL_Get.real_get_moneyflow_change(moneyflowpath)

        #选择特征工程
        #切换为e
        cur_fe=FE.FEg30eom()    
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
                    self.TodayPredict5Day_0517()
                else:
                    self.TodayPredict5Day_0517()

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

        if (caltemp>=1454 and caltemp<=1500):
            return True
        else:
            return False 