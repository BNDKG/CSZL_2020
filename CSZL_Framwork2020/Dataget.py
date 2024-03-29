#coding=utf-8

import tushare as ts
import pandas as pd
import numpy as np
import FileIO
import time
import os
import datetime
import random
from sklearn import preprocessing


class Dataget(object):
    """description of class"""
    def updatedaily(self,start_date,end_date):
        #增量更新

        #输出路径为"./Database/Dailydata.csv"

        #检查目录是否存在
        FileIO.FileIO.mkdir('./Database')

        #读取历史数据防止重复
        try:
            df_test=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
            date_list_old=df_test['trade_date'].unique().astype(str)

            xxx=1
        except Exception as e:
            #没有的情况下list为空
            date_list_old=[]
            df_test=pd.DataFrame(columns=('ts_code','trade_date','open','high','low','close','pre_close','change','pct_chg','vol','amount'))


        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        pro = ts.pro_api(token)
        date=pro.query('trade_cal', start_date=start_date, end_date=end_date)

        date=date[date["is_open"]==1]
        bufferlist=date["cal_date"]

        get_list=bufferlist[~bufferlist.isin(date_list_old)].values
        if len(get_list)<2:
            if len(get_list)==1:
                first_date=get_list[0]
                df_all=pro.daily(trade_date=first_date)
            else:
                return
        else:
            first_date=get_list[0]
            next_date=get_list[1:]

            df_all=pro.daily(trade_date=first_date)

            zcounter=0
            zall=get_list.shape[0]
            for singledate in next_date:
                zcounter+=1
                print(zcounter*100/zall)

                dec=5
                while(dec>0):
                    try:
                        time.sleep(1)
                        df = pro.daily(trade_date=singledate)
                        
                        df_all=pd.concat([df_all,df])

                        #df_last
                        #print(df_all)
                        break

                    except Exception as e:
                        dec-=1
                        time.sleep(5-dec)

                if(dec==0):
                    fsefe=1

        df_all=pd.concat([df_all,df_test])
        df_all[["trade_date"]]=df_all[["trade_date"]].astype(int)
        df_all.sort_values("trade_date",inplace=True)

        df_all=df_all.reset_index(drop=True)
        #688del
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all.to_csv('./Database/Dailydata.csv')

        dsdfsf=1

    def getDataSet(self,start_date,end_date,folderpath):
        #获取某日到某日的数据,并保存到temp中
        filename=folderpath+'DataSet'+start_date+'to'+end_date+'.csv'

        #检查目录是否存在
        FileIO.FileIO.mkdir(folderpath)
        #检查文件是否存在
        if(os.path.exists(filename)==False):       
            try:
                df_get=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
            
                df_get=df_get[df_get['trade_date']>=int(start_date)]
                df_get=df_get[df_get['trade_date']<=int(end_date)]
                df_get=df_get.reset_index(drop=True)
                df_get.to_csv(filename)
                xxx=1
                print('数据集生成完成')
            except Exception as e:
                #没有的情况下list为空
                print("错误，请先调用updatedaily或检查其他问题")
        return filename

    def updatedaily_adj_factor(self,start_date,end_date):

        #增量更新

        #输出路径为"./Database/Dailydata.csv"

        #检查目录是否存在
        FileIO.FileIO.mkdir('./Database')

        #读取历史数据防止重复
        try:
            df_test=pd.read_csv('./Database/Daily_adj_factor.csv',index_col=0,header=0)
            date_list_old=df_test['trade_date'].unique().astype(str)

            xxx=1
        except Exception as e:
            #没有的情况下list为空
            date_list_old=[]
            df_test=pd.DataFrame(columns=('ts_code','trade_date','adj_factor'))


        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        pro = ts.pro_api(token)
        date=pro.query('trade_cal', start_date=start_date, end_date=end_date)

        date=date[date["is_open"]==1]
        bufferlist=date["cal_date"]

        get_list=bufferlist[~bufferlist.isin(date_list_old)].values

        if len(get_list)<2:
            if len(get_list)==1:
                first_date=get_list[0]
                df_all=pro.adj_factor(ts_code='', trade_date=first_date)
            else:
                return
        else:

            first_date=get_list[0]
            next_date=get_list[1:]

            df_all=pro.adj_factor(ts_code='', trade_date=first_date)

            zcounter=0
            zall=get_list.shape[0]
            for singledate in next_date:
                zcounter+=1
                print(zcounter*100/zall)

                dec=5
                while(dec>0):
                    try:
                        time.sleep(1)
                        df = pro.adj_factor(ts_code='', trade_date=singledate)

                        df_all=pd.concat([df_all,df])

                        #df_last
                        #print(df_all)
                        break

                    except Exception as e:
                        dec-=1
                        time.sleep(5-dec)

                if(dec==0):
                    fsefe=1

        df_all=pd.concat([df_all,df_test])
        df_all[["trade_date"]]=df_all[["trade_date"]].astype(int)
        df_all.sort_values("trade_date",inplace=True)

        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('./Database/Daily_adj_factor.csv')

        dsdfsf=1

    def getDataSet_adj_factor(self,start_date,end_date,folderpath):
        #获取某日到某日的数据,并保存到temp中
        filename=folderpath+'Daily_adj_factor'+start_date+'to'+end_date+'.csv'

        #检查目录是否存在
        FileIO.FileIO.mkdir(folderpath)
        #检查文件是否存在
        if(os.path.exists(filename)==False):       
            try:
                df_get=pd.read_csv('./Database/Daily_adj_factor.csv',index_col=0,header=0)
            
                df_get=df_get[df_get['trade_date']>=int(start_date)]
                df_get=df_get[df_get['trade_date']<=int(end_date)]
                df_get=df_get.reset_index(drop=True)
                df_get.to_csv(filename)
                xxx=1
                print('数据集生成完成')
            except Exception as e:
                #没有的情况下list为空
                print("错误，请先调用updatedaily_adj_factor或检查其他问题")
        return filename

    def updatedaily_long_factors_old(self,start_date,end_date):

        #增量更新

        #输出路径为"./Database/Dailydata.csv"

        #检查目录是否存在
        FileIO.FileIO.mkdir('./Database')

        #读取历史数据防止重复
        try:
            df_test=pd.read_csv('./Database/Daily_long_factor.csv',index_col=0,header=0)
            date_list_old=df_test['trade_date'].unique().astype(str)

            xxx=1
        except Exception as e:
            #没有的情况下list为空
            date_list_old=[]
            df_test=pd.DataFrame(columns=('ts_code','trade_date','turnover_rate','volume_ratio','pe','pb','total_mv'))


        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        pro = ts.pro_api(token)
        date=pro.query('trade_cal', start_date=start_date, end_date=end_date)

        date=date[date["is_open"]==1]
        bufferlist=date["cal_date"]

        get_list=bufferlist[~bufferlist.isin(date_list_old)].values
        first_date=get_list[0]
        next_date=get_list[1:]

        df_all=pro.daily_basic(ts_code='', trade_date=first_date, fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb,total_mv')

        zcounter=0
        zall=get_list.shape[0]
        for singledate in next_date:
            zcounter+=1
            print(zcounter*100/zall)

            dec=5
            while(dec>0):
                try:
                    time.sleep(1)
                    #df = pro.adj_factor(ts_code='', trade_date=singledate)

                    df = pro.daily_basic(ts_code='', trade_date=singledate, fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb,total_mv')

                    df_all=pd.concat([df_all,df])

                    #df_last
                    #print(df_all)
                    break

                except Exception as e:
                    dec-=1
                    time.sleep(5-dec)

            if(dec==0):
                fsefe=1

        df_all=pd.concat([df_all,df_test])
        df_all[["trade_date"]]=df_all[["trade_date"]].astype(int)
        df_all.sort_values("trade_date",inplace=True)

        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('./Database/Daily_long_factor.csv')

        dsdfsf=1

    def updatedaily_long_factors(self,start_date,end_date):

        #增量更新

        #输出路径为"./Database/Dailydata.csv"

        #检查目录是否存在
        FileIO.FileIO.mkdir('./Database')

        #读取历史数据防止重复
        try:
            df_test=pd.read_csv('./Database/Daily_long_factor.csv',index_col=0,header=0)
            date_list_old=df_test['trade_date'].unique().astype(str)

            xxx=1
        except Exception as e:
            #没有的情况下list为空
            date_list_old=[]
            df_test=pd.DataFrame(columns=('ts_code','trade_date','turnover_rate','volume_ratio','pe','pb','ps_ttm','dv_ttm','circ_mv','total_mv'))


        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        pro = ts.pro_api(token)
        date=pro.query('trade_cal', start_date=start_date, end_date=end_date)

        date=date[date["is_open"]==1]
        bufferlist=date["cal_date"]

        get_list=bufferlist[~bufferlist.isin(date_list_old)].values
        if len(get_list)<2:
            if len(get_list)==1:
                first_date=get_list[0]
                df_all=pro.daily_basic(ts_code='', trade_date=first_date, fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb,ps_ttm,dv_ttm,circ_mv,total_mv')
            else:
                return
        else:
            first_date=get_list[0]
            next_date=get_list[1:]

            df_all=pro.daily_basic(ts_code='', trade_date=first_date, fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb,ps_ttm,dv_ttm,circ_mv,total_mv')

            zcounter=0
            zall=get_list.shape[0]
            for singledate in next_date:
                zcounter+=1
                print(zcounter*100/zall)

                dec=5
                while(dec>0):
                    try:
                        time.sleep(1)
                        df = pro.daily_basic(ts_code='', trade_date=singledate, fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb,ps_ttm,dv_ttm,circ_mv,total_mv')
                        
                        df_all=pd.concat([df_all,df])

                        #df_last
                        #print(df_all)
                        break

                    except Exception as e:
                        dec-=1
                        time.sleep(5-dec)

                if(dec==0):
                    fsefe=1

        df_all=pd.concat([df_all,df_test])
        df_all[["trade_date"]]=df_all[["trade_date"]].astype(int)
        df_all.sort_values("trade_date",inplace=True)

        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('./Database/Daily_long_factor.csv')

        dsdfsf=1

    def getDataSet_long_factor(self,start_date,end_date,folderpath):
        #获取某日到某日的数据,并保存到temp中
        filename=folderpath+'Daily_long_factor'+start_date+'to'+end_date+'.csv'

        #检查目录是否存在
        FileIO.FileIO.mkdir(folderpath)
        #检查文件是否存在
        if(os.path.exists(filename)==False):       
            try:
                df_get=pd.read_csv('./Database/Daily_long_factor.csv',index_col=0,header=0)
            
                df_get=df_get[df_get['trade_date']>=int(start_date)]
                df_get=df_get[df_get['trade_date']<=int(end_date)]
                df_get=df_get.reset_index(drop=True)
                df_get.to_csv(filename)
                xxx=1
                print('数据集生成完成')
            except Exception as e:
                #没有的情况下list为空
                print("错误，请先调用updatedaily_long_factor或检查其他问题")
        return filename

    def update_moneyflow(self,start_date,end_date):

        #增量更新

        #输出路径为"./Database/Dailydata.csv"

        #检查目录是否存在
        FileIO.FileIO.mkdir('./Database')

        #读取历史数据防止重复
        try:
            df_test=pd.read_csv('./Database/Daily_moneyflow.csv',index_col=0,header=0)
            date_list_old=df_test['trade_date'].unique().astype(str)

            xxx=1
        except Exception as e:
            #没有的情况下list为空
            date_list_old=[]
            df_test=pd.DataFrame(columns=('ts_code','trade_date','buy_sm_vol','buy_sm_amount','sell_sm_vol',
                                          'sell_sm_amount','buy_md_vol','buy_md_amount','sell_md_vol','sell_md_amount',
                                          'buy_lg_vol','buy_lg_amount','sell_lg_vol','sell_lg_amount','buy_elg_vol','buy_elg_amount',
                                          'sell_elg_vol','sell_elg_amount','net_mf_vol','net_mf_amount'))


        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        pro = ts.pro_api(token)

        date=pro.query('trade_cal', start_date=start_date, end_date=end_date)

        date=date[date["is_open"]==1]
        bufferlist=date["cal_date"]

        get_list=bufferlist[~bufferlist.isin(date_list_old)].values

        if len(get_list)<2:
            if len(get_list)==1:
                first_date=get_list[0]
                df_all=pro.moneyflow(ts_code='', trade_date=first_date)
            else:
                return
        else:

            first_date=get_list[0]
            next_date=get_list[1:]

            df_all=pro.moneyflow(ts_code='', trade_date=first_date)

            zcounter=0
            zall=get_list.shape[0]
            for singledate in next_date:
                zcounter+=1
                print(zcounter*100/zall)

                dec=5
                while(dec>0):
                    try:
                        time.sleep(1)
                        df = pro.moneyflow(ts_code='', trade_date=singledate)

                        df_all=pd.concat([df_all,df])

                        #df_last
                        #print(df_all)
                        break

                    except Exception as e:
                        dec-=1
                        time.sleep(5-dec)

                if(dec==0):
                    fsefe=1

        df_all=pd.concat([df_all,df_test])
        df_all[["trade_date"]]=df_all[["trade_date"]].astype(int)
        df_all.sort_values("trade_date",inplace=True)

        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('./Database/Daily_moneyflow.csv')

        dsdfsf=1

    def getDataSet_moneyflow(self,start_date,end_date,folderpath):
        #获取某日到某日的数据,并保存到temp中
        filename=folderpath+'Daily_moneyflow'+start_date+'to'+end_date+'.csv'

        #检查目录是否存在
        FileIO.FileIO.mkdir(folderpath)
        #检查文件是否存在
        if(os.path.exists(filename)==False):       
            try:
                df_get=pd.read_csv('./Database/Daily_moneyflow.csv',index_col=0,header=0)
            
                df_get=df_get[df_get['trade_date']>=int(start_date)]
                df_get=df_get[df_get['trade_date']<=int(end_date)]
                df_get=df_get.reset_index(drop=True)
                df_get.to_csv(filename)
                xxx=1
                print('数据集生成完成')
            except Exception as e:
                #没有的情况下list为空
                print("错误，请先调用updatedaily_moneyflow或检查其他问题")
        return filename

    def update_stk_limit(self,start_date,end_date):

        #增量更新

        #输出路径为"./Database/Dailydata.csv"

        #检查目录是否存在
        FileIO.FileIO.mkdir('./Database')

        #读取历史数据防止重复
        try:
            df_test=pd.read_csv('./Database/Daily_stk_limit.csv',index_col=0,header=0)
            date_list_old=df_test['trade_date'].unique().astype(str)

            xxx=1
        except Exception as e:
            #没有的情况下list为空
            date_list_old=[]
            df_test=pd.DataFrame(columns=('trade_date','ts_code','up_limit','down_limit'))


        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        pro = ts.pro_api(token)

        date=pro.query('trade_cal', start_date=start_date, end_date=end_date)

        date=date[date["is_open"]==1]
        bufferlist=date["cal_date"]

        get_list=bufferlist[~bufferlist.isin(date_list_old)].values

        if len(get_list)<2:
            if len(get_list)==1:
                first_date=get_list[0]
                df_all=pro.stk_limit(ts_code='', trade_date=first_date)
            else:
                return
        else:

            first_date=get_list[0]
            next_date=get_list[1:]

            df_all=pro.stk_limit(ts_code='', trade_date=first_date)

            zcounter=0
            zall=get_list.shape[0]
            for singledate in next_date:
                zcounter+=1
                print(zcounter*100/zall)

                dec=5
                while(dec>0):
                    try:
                        time.sleep(1)
                        df = pro.stk_limit(ts_code='', trade_date=singledate)

                        df_all=pd.concat([df_all,df])

                        #df_last
                        #print(df_all)
                        break

                    except Exception as e:
                        dec-=1
                        time.sleep(5-dec)

                if(dec==0):
                    fsefe=1

        df_all=pd.concat([df_all,df_test])
        df_all[["trade_date"]]=df_all[["trade_date"]].astype(int)
        df_all.sort_values("trade_date",inplace=True)

        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('./Database/Daily_stk_limit.csv')

        dsdfsf=1

    def getDataSet_stk_limit(self,start_date,end_date,folderpath):
        #获取某日到某日的数据,并保存到temp中
        filename=folderpath+'Daily_stk_limit'+start_date+'to'+end_date+'.csv'

        #检查目录是否存在
        FileIO.FileIO.mkdir(folderpath)
        #检查文件是否存在
        if(os.path.exists(filename)==False):       
            try:
                df_get=pd.read_csv('./Database/Daily_stk_limit.csv',index_col=0,header=0)
            
                df_get=df_get[df_get['trade_date']>=int(start_date)]
                df_get=df_get[df_get['trade_date']<=int(end_date)]
                df_get=df_get.reset_index(drop=True)
                df_get.to_csv(filename)
                xxx=1
                print('数据集生成完成')
            except Exception as e:
                #没有的情况下list为空
                print("错误，请先调用updatedaily_stk_limit或检查其他问题")
        return filename

    def update_concept(self):

        #增量更新

        #输出路径为"./Database/Dailydata.csv"

        #检查目录是否存在
        FileIO.FileIO.mkdir('./Database')

        #读取历史数据防止重复
        try:
            df_test=pd.read_csv('./Database/Daily_concept.csv',index_col=0,header=0)
            #date_list_old=df_test['trade_date'].unique().astype(str)

            xxx=1
        except Exception as e:
            #没有的情况下list为空
            date_list_old=[]
            df_test=pd.DataFrame(columns=('id','concept_name','ts_code','name'))


        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        pro = ts.pro_api(token)

        #先拿到列表
        df_list=pro.concept(src='ts')

        print(df_list)
        allcodes=df_list['code']
        le = preprocessing.LabelEncoder()
        le.fit(allcodes)
        aas=le.transform(allcodes)
        print(aas)

        data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,list_date,market')

        get_list=data['ts_code'].values

        if len(get_list)<2:
            if len(get_list)==1:
                first_code=get_list[0]                
                df_all=pro.concept_detail(ts_code = first_code)
            else:
                return
        else:

            first_code=get_list[0]
            next_date=get_list[1:]

            df_all=pro.concept_detail(ts_code = first_code)
            all=len(get_list)
            zcounter2=0
            for getcode in next_date:
                try:
                    time.sleep(0.1)
                    curconcept=pro.concept_detail(ts_code = getcode)
                    df_all=pd.concat([df_all,curconcept])
                    print(zcounter2/(all+1))


                    zcounter2+=1
                    #if(zcounter2>10):
                    #    break
                    fadfsas=1

                except Exception as e:
                    time.sleep(5-dec)

        #print(df_all)

        df_all.to_csv('./Database/Daily_concept.csv',encoding="utf_8_sig")


        dsdfsf=1

    def getDataSet_concept(self):
        #获取某日到某日的数据,并保存到temp中
        filename='./temp/'+'Daily_concept.csv'

        #检查目录是否存在
        FileIO.FileIO.mkdir('./temp/')
        #检查文件是否存在
        if(os.path.exists(filename)==False):       
            try:
                df_get=pd.read_csv('./Database/Daily_concept.csv',index_col=0,header=0)
            
                #df_get=df_get[df_get['trade_date']>int(start_date)]
                #df_get=df_get[df_get['trade_date']<int(end_date)]
                #df_get=df_get.reset_index(drop=True)
                df_get.to_csv(filename,encoding="utf_8_sig")
                xxx=1
                print('数据集生成完成')
            except Exception as e:
                #没有的情况下list为空
                print("错误，请先调用update_concept或检查其他问题")
        return filename

    def update_basic(self):
        #获取基本数据

        FileIO.FileIO.mkdir('./Database')

        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        pro = ts.pro_api(token)
        data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,list_date,market')
        data2 = df = pro.concept(src='ts')

        data.to_csv('./Database/Daily_basic.csv',encoding="utf_8_sig")
        data2.to_csv('./Database/Daily_conceptlist.csv',encoding="utf_8_sig")

    def getDataSet_conceptmixed(self):
        #获取某日到某日的数据,并保存到temp中
        filename='./temp/'+'Daily_conceptmixed.csv'

        #检查目录是否存在
        FileIO.FileIO.mkdir('./temp/')
        #检查文件是否存在
        if(os.path.exists(filename)==False):       
            try:
                #df_get=pd.read_csv('./Database/Daily_conceptlist.csv',index_col=0,header=0)

                df_conceptlist=pd.read_csv('./Database/Daily_conceptlist.csv',index_col=0,header=0)
                df_concept=pd.read_csv('./Database/Daily_concept.csv',index_col=0,header=0)

                asda = df_conceptlist['code'].values
                asda = np.insert(asda, 0, "ts_code")
                default=np.zeros(len(asda),dtype=int).reshape(1,len(asda))
                #default = np.insert(default, 0, "1")

                listchanger_zero=pd.DataFrame(data=default,columns=asda)
                listchanger_zero['ts_code']= listchanger_zero['ts_code'].astype(np.str)

                #listchanger=listchanger.append(default, ignore_index=True)

                print(listchanger_zero)
                df_all2=pd.DataFrame(columns=asda)

                last_name="000000.sz"
                curconcept=listchanger_zero.copy()
                counterz=0
                for curdata in df_concept.iterrows():
                    cur_name=curdata[1]['ts_code']
                    if(last_name!=cur_name):
                        #如果没有换名字就不新创建

                        df_all2=pd.concat([df_all2,curconcept])
                        curconcept=listchanger_zero.copy()
               
                        d_index = list(curconcept.columns).index('ts_code')
                        curconcept.iloc[0,d_index] = cur_name

                        #print(df_all2)

                    cur_conceptid=curdata[1]['id']
                    id_index = list(curconcept.columns).index(cur_conceptid)
                    curconcept.iloc[0,id_index] = 1
                    #print(curconcept)
                    #curconcept[0]["ts_code"]=cur_name
                    counterz+=1

                    if(counterz%200==0):
                        print(counterz/df_concept.shape[0])
                    last_name=cur_name

                print(df_all2)
                df_all2.reset_index(drop=True, inplace=True)
                df_all2=df_all2.drop(index=[0],axis=0)
                #df_get=df_get[df_get['trade_date']>int(start_date)]
                #df_get=df_get[df_get['trade_date']<int(end_date)]
                #df_get=df_get.reset_index(drop=True)
                df_all2.to_csv(filename)
                xxx=1
                print('数据集生成完成')
            except Exception as e:
                #没有的情况下list为空
                print("错误，请检查其他问题")
        return filename

    def getDataSet_basic(self):
        #获取某日到某日的数据,并保存到temp中
        filename='./temp/'+'Daily_basic.csv'
        filename2='./temp/'+'Daily_conceptlist.csv'

        #检查目录是否存在
        FileIO.FileIO.mkdir('./temp/')
        #检查文件是否存在
        if(os.path.exists(filename)==False):       
            try:
                df_get=pd.read_csv('./Database/Daily_basic.csv',index_col=0,header=0)
                df_get2=pd.read_csv('./Database/Daily_conceptlist.csv',index_col=0,header=0)
            
                #df_get=df_get[df_get['trade_date']>int(start_date)]
                #df_get=df_get[df_get['trade_date']<int(end_date)]
                #df_get=df_get.reset_index(drop=True)
                df_get.to_csv(filename,encoding="utf_8_sig")
                df_get2.to_csv(filename2,encoding="utf_8_sig")
                xxx=1
                print('数据集生成完成')
            except Exception as e:
                #没有的情况下list为空
                print("错误，请先调用update_basic或检查其他问题")
        return filename,filename2

    def get_history_dateset(self):

        #检查目录是否存在
        FileIO.FileIO.mkdir('./temp_real')

        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()
        pro = ts.pro_api(token)
        #生成需要的数据集
        nowTime=datetime.datetime.now()
        delta = datetime.timedelta(days=83)
        delta_one = datetime.timedelta(days=1)
        nowTime=nowTime-delta_one
        month_ago = nowTime - delta
        month_ago_next=month_ago+delta_one
        month_fst=month_ago_next.strftime('%Y%m%d')  
        month_sec=nowTime.strftime('%Y%m%d')  
        month_thd=month_ago.strftime('%Y%m%d')      

        #刷新数据库
        self.updatedaily(month_fst,month_sec)

        #刷新复权因子
        self.updatedaily_adj_factor(month_fst,month_sec)

        #刷新资金流入数据
        #self.update_moneyflow(month_fst,month_sec)

        savename='./temp_real/'+'dataset_'+month_fst+'_'+month_sec+'.csv'
        savename_adj='./temp_real/'+'dataset_adj_'+month_fst+'_'+month_sec+'.csv'
        #savename_moneyflow='./temp_real/'+'moneyflow_'+month_fst+'_'+month_sec+'.csv'

        if(os.path.exists(savename)==False):    

            df_get=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
            
            df_get=df_get[df_get['trade_date']>int(month_fst)]
            df_get=df_get[df_get['trade_date']<=int(month_sec)]
            df_get=df_get.reset_index(drop=True)

            df_get['ts_code']=df_get['ts_code'].map(lambda x : x[:-3])
            df_get.drop(['vol','change'],axis=1,inplace=True)
        
            df_get.to_csv(savename)

            df_get2=pd.read_csv('./Database/Daily_adj_factor.csv',index_col=0,header=0)
            
            df_get2=df_get2[df_get2['trade_date']>int(month_fst)]
            df_get2=df_get2[df_get2['trade_date']<=int(month_sec)]
            df_get2=df_get2.reset_index(drop=True)
            df_get2.to_csv(savename_adj)

            #df_get3=pd.read_csv('./Database/Daily_moneyflow.csv',index_col=0,header=0)
            
            #df_get3=df_get3[df_get3['trade_date']>int(month_fst)]
            #df_get3=df_get3[df_get3['trade_date']<=int(month_sec)]
            #df_get3=df_get3.reset_index(drop=True)
            #df_get3.to_csv(savename_moneyflow)


        return savename,savename_adj,savename_moneyflow

    def get_history_dateset_two(self):

        #检查目录是否存在
        FileIO.FileIO.mkdir('./temp_real')

        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()
        pro = ts.pro_api(token)
        #生成需要的数据集
        nowTime=datetime.datetime.now()
        delta = datetime.timedelta(days=83)
        delta_one = datetime.timedelta(days=1)
        nowTime=nowTime-delta_one
        month_ago = nowTime - delta
        month_ago_next=month_ago+delta_one
        month_fst=month_ago_next.strftime('%Y%m%d')  
        month_sec=nowTime.strftime('%Y%m%d')  
        month_thd=month_ago.strftime('%Y%m%d')      

        #刷新数据库
        self.updatedaily(month_fst,month_sec)

        #刷新复权因子
        self.updatedaily_adj_factor(month_fst,month_sec)

        #刷新资金流入数据
        #self.update_moneyflow(month_fst,month_sec)

        savename='./temp_real/'+'dataset_'+month_fst+'_'+month_sec+'.csv'
        savename_adj='./temp_real/'+'dataset_adj_'+month_fst+'_'+month_sec+'.csv'
        #savename_moneyflow='./temp_real/'+'moneyflow_'+month_fst+'_'+month_sec+'.csv'

        if(os.path.exists(savename)==False):    

            df_get=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
            
            df_get=df_get[df_get['trade_date']>int(month_fst)]
            df_get=df_get[df_get['trade_date']<=int(month_sec)]
            df_get=df_get.reset_index(drop=True)

            df_get['ts_code']=df_get['ts_code'].map(lambda x : x[:-3])
            df_get.drop(['vol','change'],axis=1,inplace=True)
        
            df_get.to_csv(savename)

            df_get2=pd.read_csv('./Database/Daily_adj_factor.csv',index_col=0,header=0)
            
            df_get2=df_get2[df_get2['trade_date']>int(month_fst)]
            df_get2=df_get2[df_get2['trade_date']<=int(month_sec)]
            df_get2=df_get2.reset_index(drop=True)
            df_get2.to_csv(savename_adj)

            #df_get3=pd.read_csv('./Database/Daily_moneyflow.csv',index_col=0,header=0)
            
            #df_get3=df_get3[df_get3['trade_date']>int(month_fst)]
            #df_get3=df_get3[df_get3['trade_date']<=int(month_sec)]
            #df_get3=df_get3.reset_index(drop=True)
            #df_get3.to_csv(savename_moneyflow)


        return savename,savename_adj

    def get_history_dateset_three(self):

        #检查目录是否存在
        FileIO.FileIO.mkdir('./temp_real')

        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()
        pro = ts.pro_api(token)
        #生成需要的数据集
        nowTime=datetime.datetime.now()
        delta = datetime.timedelta(days=83)
        delta_one = datetime.timedelta(days=1)
        nowTime=nowTime-delta_one
        month_ago = nowTime - delta
        month_ago_next=month_ago+delta_one
        month_fst=month_ago_next.strftime('%Y%m%d')  
        month_sec=nowTime.strftime('%Y%m%d')  
        month_thd=month_ago.strftime('%Y%m%d')      

        #刷新数据库
        self.updatedaily(month_fst,month_sec)

        #刷新复权因子
        self.updatedaily_adj_factor(month_fst,month_sec)

        #刷新长线指标？
        self.updatedaily_long_factors(month_fst,month_sec)

        #刷新资金流入数据
        #self.update_moneyflow(month_fst,month_sec)

        savename='./temp_real/'+'dataset_'+month_fst+'_'+month_sec+'.csv'
        savename_adj='./temp_real/'+'dataset_adj_'+month_fst+'_'+month_sec+'.csv'
        savename_long='./temp_real/'+'dataset_long_'+month_fst+'_'+month_sec+'.csv'
        #savename_moneyflow='./temp_real/'+'moneyflow_'+month_fst+'_'+month_sec+'.csv'

        if(os.path.exists(savename)==False):    

            df_get=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
            
            df_get=df_get[df_get['trade_date']>int(month_fst)]
            df_get=df_get[df_get['trade_date']<=int(month_sec)]
            df_get=df_get.reset_index(drop=True)

            df_get['ts_code']=df_get['ts_code'].map(lambda x : x[:-3])
            df_get.drop(['vol','change'],axis=1,inplace=True)
        
            df_get.to_csv(savename)

            df_get2=pd.read_csv('./Database/Daily_adj_factor.csv',index_col=0,header=0)
            
            df_get2=df_get2[df_get2['trade_date']>int(month_fst)]
            df_get2=df_get2[df_get2['trade_date']<=int(month_sec)]
            df_get2=df_get2.reset_index(drop=True)
            df_get2.to_csv(savename_adj)

            df_get3=pd.read_csv('./Database/Daily_long_factor.csv',index_col=0,header=0)
            
            df_get3=df_get3[df_get3['trade_date']>int(month_fst)]
            df_get3=df_get3[df_get3['trade_date']<=int(month_sec)]
            df_get3=df_get3.reset_index(drop=True)
            df_get3.to_csv(savename_long)

            #df_get3=pd.read_csv('./Database/Daily_moneyflow.csv',index_col=0,header=0)
            
            #df_get3=df_get3[df_get3['trade_date']>int(month_fst)]
            #df_get3=df_get3[df_get3['trade_date']<=int(month_sec)]
            #df_get3=df_get3.reset_index(drop=True)
            #df_get3.to_csv(savename_moneyflow)


        return savename,savename_adj,savename_long

    def get_history_dateset_four(self):

        #检查目录是否存在
        FileIO.FileIO.mkdir('./temp_real')

        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()
        pro = ts.pro_api(token)
        #生成需要的数据集
        nowTime=datetime.datetime.now()
        delta = datetime.timedelta(days=63)
        delta_one = datetime.timedelta(days=1)
        nowTime=nowTime-delta_one
        month_ago = nowTime - delta
        month_ago_next=month_ago+delta_one
        month_fst=month_ago_next.strftime('%Y%m%d')  
        month_sec=nowTime.strftime('%Y%m%d')  
        month_thd=month_ago.strftime('%Y%m%d')      

        #刷新数据库
        self.updatedaily(month_fst,month_sec)

        #刷新复权因子
        self.updatedaily_adj_factor(month_fst,month_sec)

        #刷新长线指标？
        self.updatedaily_long_factors(month_fst,month_sec)

        #刷新资金流入数据
        self.update_moneyflow(month_fst,month_sec)

        savename='./temp_real/'+'dataset_'+month_fst+'_'+month_sec+'.csv'
        savename_adj='./temp_real/'+'dataset_adj_'+month_fst+'_'+month_sec+'.csv'
        savename_long='./temp_real/'+'dataset_long_'+month_fst+'_'+month_sec+'.csv'
        savename_moneyflow='./temp_real/'+'moneyflow_'+month_fst+'_'+month_sec+'.csv'

        if(os.path.exists(savename)==False):    

            df_get=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
            
            df_get=df_get[df_get['trade_date']>int(month_fst)]
            df_get=df_get[df_get['trade_date']<=int(month_sec)]
            df_get=df_get.reset_index(drop=True)

            df_get['ts_code']=df_get['ts_code'].map(lambda x : x[:-3])
            df_get.drop(['vol','change'],axis=1,inplace=True)
        
            df_get.to_csv(savename)

            df_get2=pd.read_csv('./Database/Daily_adj_factor.csv',index_col=0,header=0)
            
            df_get2=df_get2[df_get2['trade_date']>int(month_fst)]
            df_get2=df_get2[df_get2['trade_date']<=int(month_sec)]
            df_get2=df_get2.reset_index(drop=True)
            df_get2.to_csv(savename_adj)

            df_get3=pd.read_csv('./Database/Daily_long_factor.csv',index_col=0,header=0)
            
            df_get3=df_get3[df_get3['trade_date']>int(month_fst)]
            df_get3=df_get3[df_get3['trade_date']<=int(month_sec)]
            df_get3=df_get3.reset_index(drop=True)
            df_get3.to_csv(savename_long)

            df_get3=pd.read_csv('./Database/Daily_moneyflow.csv',index_col=0,header=0)
            
            df_get3=df_get3[df_get3['trade_date']>int(month_fst)]
            df_get3=df_get3[df_get3['trade_date']<=int(month_sec)]
            df_get3=df_get3.reset_index(drop=True)
            df_get3.to_csv(savename_moneyflow)


        return savename,savename_adj,savename_long,savename_moneyflow

    def real_get_change(self,path_his):

        df_history=pd.read_csv(path_his,index_col=0,header=0)

        #df_history['trade_date'] = df_history['trade_date'].astype(str)

        #df_history=df_history[df_history['trade_date']!=20200619]


        codelistbuffer=df_history['ts_code']
        codelistbuffer=codelistbuffer.unique()

        codelist=codelistbuffer.tolist()

        code_counter=0
        bufferlist=[]
        df_real=[]

        printcounter=0.0
        for curcode in codelist:

            curcode_str=str(curcode).zfill(6)
            bufferlist.append(curcode_str)
            code_counter+=1
            if(code_counter>=500):
                if(len(df_real)):
                    wrongcounter=0
                    while(1):
                        try:
                            df_real2=[]
                            df_real2=ts.get_realtime_quotes(bufferlist)
                            df_real=df_real.append(df_real2)
                            break
                        except Exception as e:
                            sleeptime2=random.randint(100,199)
                            time.sleep(sleeptime2/40)
                            wrongcounter+=1
                            if(wrongcounter>10):
                                break
                else:
                    #df_real=ts.get_realtime_quotes(bufferlist)
                    wrongcounter=0
                    while(1):
                        try:
                            df_real=ts.get_realtime_quotes(bufferlist)
                            break
                        except Exception as e:
                            sleeptime2=random.randint(100,199)
                            time.sleep(sleeptime2/40)
                            wrongcounter+=1
                            if(wrongcounter>10):
                                break
                bufferlist=[]            
                code_counter=0
                sleeptime=random.randint(100,199)
                time.sleep(sleeptime/40)
                print(printcounter/len(codelist))

            printcounter+=1
        time.sleep(2)
        if(len(bufferlist)):
            wrongcounter=0
            while(1):
                try:
                    df_real2=[]
                    df_real2=ts.get_realtime_quotes(bufferlist)
                    df_real=df_real.append(df_real2)
                    break
                except Exception as e:
                    sleeptime2=random.randint(100,199)
                    time.sleep(sleeptime2/40)
                    wrongcounter+=1
                    if(wrongcounter>10):
                        break

        #df_real=ts.get_realtime_quotes(['600839','000980','000981'])
        #df_real2=ts.get_realtime_quotes(['000010','600000','600010'])
        #df_real=df_real.append(df_real2)

        #print(df_real)
        #'tomorrow_chg'
        df_real.drop(['name','bid','ask','volume','b1_v','b2_v','b3_v','b4_v','b5_v','b1_p','b2_p','b3_p','b4_p','b5_p'],axis=1,inplace=True)
        df_real.drop(['a1_v','a2_v','a3_v','a4_v','a5_v','a1_p','a2_p','a3_p','a4_p','a5_p'],axis=1,inplace=True)
        df_real.drop(['time'],axis=1,inplace=True)

        df_real['amount'] = df_real['amount'].apply(float)
        df_real['amount']=df_real['amount']/1000

        #df[txt] = df[txt].map(lambda x : x[:-2])

        df_real['date']=df_real['date'].map(lambda x : x[:4]+x[5:7]+x[8:10])
    
        df_real['price'] = df_real['price'].apply(float)
        df_real['pre_close'] = df_real['pre_close'].apply(float)

        df_real['pct_chg']=(df_real['price']-df_real['pre_close'])*100/(df_real['pre_close'])


        df_real=df_real.rename(columns={'price':'close','date':'trade_date','code':'ts_code'})

        df_real.to_csv("real_buffer.csv")
        df_real=pd.read_csv("real_buffer.csv",index_col=0,header=0)
    
    
        cols = list(df_history)

        df_history=df_history.append(df_real,sort=False)
        #print(df_history)
        df_history = df_history.loc[:, cols]


        df_history=df_history.reset_index(drop=True)
        #print(df_history)
        #print(df_real)
        df_history.to_csv("real_now.csv")

        dsfesf=1

    def real_get_adj_change(self,path_adj):

        df_history=pd.read_csv(path_adj,index_col=0,header=0)

        df_history['ts_code']=df_history['ts_code'].map(lambda x : x[:-3])
        df_history.to_csv("real_adj_now.csv")

        dsfesf=1

    def real_get_long_change(self,path_long):

        df_history=pd.read_csv(path_long,index_col=0,header=0)

        df_history['ts_code']=df_history['ts_code'].map(lambda x : x[:-3])
        df_history.to_csv("real_long_now.csv")

        dsfesf=1

    def real_get_moneyflow_change(self,path_moneyflow):

        df_history=pd.read_csv(path_moneyflow,index_col=0,header=0)

        df_history['ts_code']=df_history['ts_code'].map(lambda x : x[:-3])
        df_history.to_csv("real_moneyflow_now.csv")

        dsfesf=1

    def get_rzrq(self):
        #获取两融信息

        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        pro = ts.pro_api(token)

        date=pro.query('trade_cal', start_date='20130105', end_date='20200105')
        date=date[date["is_open"]==1]
        bufferlist=date["cal_date"]
        get_list=bufferlist.values

        df_all=pro.margin(trade_date='20130104')
        for day in get_list:      
            try:
                df = pro.margin(trade_date=day)
                df_all=pd.concat([df_all,df])
                time.sleep(1)
                print(day)

            except Exception as e:
                df_all.to_csv('./Daily2rong.csv')

        df_all.to_csv('./Daily2rong.csv')

    def get_xsjj(self):
        #获取限售解禁

        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        pro = ts.pro_api(token)

        date=pro.query('trade_cal', start_date='20130105', end_date='20200105')
        date=date[date["is_open"]==1]
        bufferlist=date["cal_date"]
        get_list=bufferlist.values

        df_all=pro.share_float(ann_date='20130104')
        for day in get_list:      
            try:
                df = pro.share_float(ann_date=day)
                df_all=pd.concat([df_all,df])
                time.sleep(1)
                print(day)

            except Exception as e:
                df_all.to_csv('./float2jiejing.csv')

        df_all.to_csv('./float2jiejing.csv',encoding = 'gbk')

    def testoffound(self):
        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()
        pro = ts.pro_api(token)

        #df = pro.fund_nav(end_date='20170101')
        #df = pro.fund_basic(market='O')
        #df.to_csv('foundtest3.csv', encoding='utf_8_sig')


        #读取历史数据防止重复

        #读取历史数据防止重复
        try:
            df_test2=pd.read_csv('./Database/Found.csv',index_col=0,header=0)
            df_test2[["end_date"]]=df_test2[["end_date"]].astype(int)
            date_list_old=df_test2['end_date'].unique().astype(str)
            print(date_list_old)

            xxx=1
        except Exception as e:
            #没有的情况下list为空
            date_list_old=[]
            #df_test=pd.DataFrame(columns=('trade_date','ts_code','up_limit','down_limit'))


        xxx=1


        date=pro.query('trade_cal', start_date="20130101", end_date="20210122")


        date=date[date["is_open"]==1]
        bufferlist=date["cal_date"]
        print(bufferlist)
        get_list=bufferlist[~bufferlist.isin(date_list_old)].values

        if len(get_list)<2:
            if len(get_list)==1:
                first_date=get_list[0]
                df_all=pro.fund_nav(end_date=first_date)
            else:
                return
        else:

            first_date=get_list[0]
            next_date=get_list[1:]

            df_all=pro.fund_nav(end_date=first_date)

            zcounter=0
            zall=get_list.shape[0]
            for singledate in next_date:
                zcounter+=1
                print(zcounter*100/zall)

                dec=5
                while(dec>0):
                    try:
                        time.sleep(1)
                        df = pro.fund_nav(end_date=singledate)
                        df[["end_date"]]=df[["end_date"]].astype(int)
                        df_all=pd.concat([df_all,df])

                        #df_last
                        #print(df_all)
                        break

                    except Exception as e:
                        dec-=1
                        time.sleep(5-dec)

                if(dec==0):
                    fsefe=1

            df_all=pd.concat([df_all,df_test2])
            #df_all.to_csv('./Database/Foundtest.csv')
            #print(df_all)
            df_all[["end_date"]]=df_all[["end_date"]].astype(int)
            df_all.sort_values("end_date",inplace=True)

            df_all=df_all.reset_index(drop=True)

            df_all.to_csv('./Database/Found.csv')
        asdfasf=1

    def testoffound2(self):

        df_all=pd.read_csv("./Database/Found.csv",index_col=0,header=0)
        df_all['tomorrow_adj_nav']=df_all.groupby('ts_code')['adj_nav'].shift(-1)
        df_all=df_all[df_all['adj_nav']!=0]
        df_all['pct_chg']=(df_all['tomorrow_adj_nav']-df_all['adj_nav'])/df_all['adj_nav']
        #df_all['groptest']=df_all.groupby('ts_code')['pct_chg'].max()
        df_all.to_csv('./Database/Found2.csv')

        sdfasfsad=1

    def testoffound3(self):

        df_all=pd.read_csv("./Database/Found2.csv",index_col=0,header=0)
    

        test2=df_all.groupby('ts_code')['pct_chg'].sum()
        test2=pd.DataFrame({'rank_sum':test2})
        #test2=test2[test2['rank_sum']>0]
        print(test2)

        df_all['goal_rank']=df_all.groupby('ann_date')['pct_chg'].rank(pct=True)

        #150230.SZ 003889.OF 159928.SZ
        test=df_all[df_all['ts_code']=="159919.SZ"]
        test=test[['ann_date','goal_rank']]
        #test=test[['ann_date','goal_rank','pct_chg']]
        df_all.rename(columns={'goal_rank':'my_rank'},inplace=True)

        df_all=pd.merge(df_all, test, how='left', on=['ann_date'])

        df_all['differ_rank']=df_all['my_rank']-df_all['goal_rank']
        df_all['differ_rank']=df_all['differ_rank'].abs()

        test3=df_all.groupby('ts_code')['differ_rank'].sum()
        #test3=pd.DataFrame({'differ_rank_sum':test3})
        test5=df_all.groupby('ts_code')['differ_rank'].count()
        test5=pd.DataFrame({'differ_rank_count':test5,'differ_rank_sum':test3})

        test3=pd.merge(test2, test5, how='left', on=['ts_code'])

        test3['percent_rank']=test3['rank_sum']/(test3['differ_rank_count']+1)*100
        test3['differ_rank_sum']=test3['differ_rank_sum']/(test3['differ_rank_count']+1)*100

        #bad = pd.DataFrame({'bad':bad})
        #regroup =  total.merge(bad,left_index=True,right_index=True, how='left')

        test4=pd.read_csv("./Database/foundforname.csv",index_col=0,header=0)
        test3=pd.merge(test3, test4, how='left', on=['ts_code'])

        test3.to_csv('./Database/test3.csv',encoding="utf_8_sig")
        test2.to_csv('./Database/test2.csv')

        #test.to_csv('./Database/test.csv')
        #print(test)
        df_all.to_csv('./Database/Found3.csv')
        sdfasfsad=1

    def get_baseline(self,basecode='000905.SH'):

        #000001.SH 上证 000016.SH 50 000688.SH 科创50 000905.SH 中证500 399006.SZ 创业板指
        #399300.SZ 300 000852.SH 1000 

        savedir='./Database/indexdata'
        #检查目录是否存在
        FileIO.FileIO.mkdir(savedir)

        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        pro = ts.pro_api(token)


        df = pro.index_daily(ts_code=basecode)

        savepth=savedir+'/'+basecode+'.csv'
        df.to_csv(savepth)


        return savepth
