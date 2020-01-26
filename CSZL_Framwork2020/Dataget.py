#coding=utf-8

import tushare as ts
import pandas as pd
import FileIO
import time
import os
import datetime
import random



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
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all.to_csv('./Database/Dailydata.csv')

        dsdfsf=1

    def getDataSet(self,start_date,end_date):
        #获取某日到某日的数据,并保存到temp中
        filename='./temp/'+'DataSet'+start_date+'to'+end_date+'.csv'

        #检查目录是否存在
        FileIO.FileIO.mkdir('./temp/')
        #检查文件是否存在
        if(os.path.exists(filename)==False):       
            try:
                df_get=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
            
                df_get=df_get[df_get['trade_date']>int(start_date)]
                df_get=df_get[df_get['trade_date']<int(end_date)]
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

    def getDataSet_adj_factor(self,start_date,end_date):
        #获取某日到某日的数据,并保存到temp中
        filename='./temp/'+'Daily_adj_factor'+start_date+'to'+end_date+'.csv'

        #检查目录是否存在
        FileIO.FileIO.mkdir('./temp/')
        #检查文件是否存在
        if(os.path.exists(filename)==False):       
            try:
                df_get=pd.read_csv('./Database/Daily_adj_factor.csv',index_col=0,header=0)
            
                df_get=df_get[df_get['trade_date']>int(start_date)]
                df_get=df_get[df_get['trade_date']<int(end_date)]
                df_get=df_get.reset_index(drop=True)
                df_get.to_csv(filename)
                xxx=1
                print('数据集生成完成')
            except Exception as e:
                #没有的情况下list为空
                print("错误，请先调用updatedaily_adj_factor或检查其他问题")
        return filename

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
            df_test=pd.DataFrame(columns=('ts_code','trade_date','turnover_rate','volume_ratio','pe','pb'))


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

        df_all=pro.daily_basic(ts_code='', trade_date=first_date, fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb')

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

                    df = pro.daily_basic(ts_code='', trade_date=singledate, fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb')

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

    def getDataSet_long_factor(self,start_date,end_date):
        #获取某日到某日的数据,并保存到temp中
        filename='./temp/'+'Daily_long_factor'+start_date+'to'+end_date+'.csv'

        #检查目录是否存在
        FileIO.FileIO.mkdir('./temp/')
        #检查文件是否存在
        if(os.path.exists(filename)==False):       
            try:
                df_get=pd.read_csv('./Database/Daily_long_factor.csv',index_col=0,header=0)
            
                df_get=df_get[df_get['trade_date']>int(start_date)]
                df_get=df_get[df_get['trade_date']<int(end_date)]
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

    def getDataSet_moneyflow(self,start_date,end_date):
        #获取某日到某日的数据,并保存到temp中
        filename='./temp/'+'Daily_moneyflow'+start_date+'to'+end_date+'.csv'

        #检查目录是否存在
        FileIO.FileIO.mkdir('./temp/')
        #检查文件是否存在
        if(os.path.exists(filename)==False):       
            try:
                df_get=pd.read_csv('./Database/Daily_moneyflow.csv',index_col=0,header=0)
            
                df_get=df_get[df_get['trade_date']>int(start_date)]
                df_get=df_get[df_get['trade_date']<int(end_date)]
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

    def getDataSet_stk_limit(self,start_date,end_date):
        #获取某日到某日的数据,并保存到temp中
        filename='./temp/'+'Daily_stk_limit'+start_date+'to'+end_date+'.csv'

        #检查目录是否存在
        FileIO.FileIO.mkdir('./temp/')
        #检查文件是否存在
        if(os.path.exists(filename)==False):       
            try:
                df_get=pd.read_csv('./Database/Daily_stk_limit.csv',index_col=0,header=0)
            
                df_get=df_get[df_get['trade_date']>int(start_date)]
                df_get=df_get[df_get['trade_date']<int(end_date)]
                df_get=df_get.reset_index(drop=True)
                df_get.to_csv(filename)
                xxx=1
                print('数据集生成完成')
            except Exception as e:
                #没有的情况下list为空
                print("错误，请先调用updatedaily_stk_limit或检查其他问题")
        return filename

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

        savename='./temp_real/'+'dataset_'+month_fst+'_'+month_sec+'.csv'

        savename_adj='./temp_real/'+'dataset_adj_'+month_fst+'_'+month_sec+'.csv'

        if(os.path.exists(savename)==False):    

            df_get=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
            
            df_get=df_get[df_get['trade_date']>int(month_fst)]
            df_get=df_get[df_get['trade_date']<=int(month_sec)]
            df_get=df_get.reset_index(drop=True)

            df_get2=pd.read_csv('./Database/Daily_adj_factor.csv',index_col=0,header=0)
            
            df_get2=df_get2[df_get2['trade_date']>int(month_fst)]
            df_get2=df_get2[df_get2['trade_date']<=int(month_sec)]
            df_get2=df_get2.reset_index(drop=True)
            df_get2.to_csv(savename_adj)

            df_get['ts_code']=df_get['ts_code'].map(lambda x : x[:-3])
            df_get.drop(['vol','change'],axis=1,inplace=True)

            
            df_get.to_csv(savename)

        return savename,savename_adj

    def real_get_change(self,path_his):

        df_history=pd.read_csv(path_his,index_col=0,header=0)

        #df_history['trade_date'] = df_history['trade_date'].astype(str)

        #df_history=df_history[df_history['trade_date']!='20191206']


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
                    df_real2=ts.get_realtime_quotes(bufferlist)
                    df_real=df_real.append(df_real2)
                else:
                    df_real=ts.get_realtime_quotes(bufferlist)
                bufferlist=[]            
                code_counter=0
                sleeptime=random.randint(50,99)
                time.sleep(sleeptime/400)
                print(printcounter/len(codelist))

            printcounter+=1

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

        df_history = df_history.ix[:, cols]


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

