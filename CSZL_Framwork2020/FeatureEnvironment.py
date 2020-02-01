#coding=utf-8
import pandas as pd
import numpy as np
import sys
import os

import datetime


class FEbase(object):
    """description of class"""
    def __init__(self, **kwargs):
        pass


    def create(self,*DataSetName):
        #print (self.__class__.__name__)
        (filepath, tempfilename) = os.path.split(DataSetName[0])
        (filename, extension) = os.path.splitext(tempfilename)

        #bufferstring='savetest2017.csv'
        bufferstringoutput=filepath+'/'+filename+'_'+self.__class__.__name__+extension

        if(os.path.exists(bufferstringoutput)==False):    

            #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
            df_all=self.core(DataSetName)
            df_all.to_csv(bufferstringoutput)

        return bufferstringoutput

    def core(self,df_all,Data_adj_name=''):
        return df_all

    def real_FE():
        return 0

class FE3(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        #加入gap_day特征
        start=df_all['trade_date'].apply(str)[0]
        end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        xxx=pd.date_range(start,end)

        df = pd.DataFrame(xxx)
        df.columns = ['trade_date']
        df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        yyy=df_all['trade_date']
        zzz2=yyy.unique()
        df_2=pd.DataFrame(zzz2)
        df_2.columns = ['trade_date']
        df_2['day_flag']=1
    
        result = pd.merge(df, df_2, how='left', on=['trade_date'])
        result['day_flag2']=result['day_flag'].shift(-1)
        result['gap_day']=0

        result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        #tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        #tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.4) & (4.6<df_all['pct_chg']),'high_stop']=1


        #真实价格范围(区分实际股价高低)
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self,gap_day):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #加入间隔
        df_all['gap_day']=gap_day

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#

        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.5) & (4.5<df_all['pct_chg']),'high_stop']=1


        #真实价格范围
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']!=month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEg30(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2



        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)
        #df_all.drop(['20_pct','real_price_20min','20_pct_max','real_price_20max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)

        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEg30b(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.78<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2



        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)
        #df_all.drop(['20_pct','real_price_20min','20_pct_max','real_price_20max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)

        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        #df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)


        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEg30c(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.78<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#

        df_all=FEsingle.PredictDays(df_all,5)

        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,10)

        df_all=FEsingle.AmountChgRank(df_all,10)    

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ###加入前日open
        #df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        #df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        #df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        #df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ###加入前日open
        #df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        #df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        #df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        #df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        #df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)


        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEg30d(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.78<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#

        df_all=FEsingle.PredictDays(df_all,5)

        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,10)

        df_all=FEsingle.AmountChgRank(df_all,10)    

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ###加入前日open
        #df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        #df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        #df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        #df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ###加入前日open
        #df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        #df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        #df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        #df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        #df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)


        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEg30e(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.78<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#

        df_all=FEsingle.PredictDays(df_all,5)

        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,10)

        df_all=FEsingle.AmountChgRank(df_all,10)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):
        #新模型预定版本

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,10)

        #print(df_all)

        df_all=FEsingle.AmountChgRank(df_all,10)

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        #df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)


        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEg30n(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_moneyflow_all=pd.read_csv(DataSetName[3],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_moneyflow_all, how='inner', on=['ts_code','trade_date'])
        
        df_all['moneyflow']=df_all['buy_lg_amount']+df_all['buy_md_amount']+df_all['buy_elg_amount']-df_all['sell_lg_amount']-df_all['sell_md_amount']-df_all['sell_elg_amount']

        df_all.drop(['buy_lg_amount','buy_sm_vol','buy_sm_amount','sell_sm_vol',
                     'sell_sm_amount','buy_md_vol','buy_md_amount','sell_md_vol','sell_md_amount',
                     'buy_lg_vol','sell_lg_vol','sell_lg_amount','buy_elg_vol','buy_elg_amount',
                     'sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.78<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,15,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,15,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#

        df_all=FEsingle.PredictDays(df_all,5)

        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//2


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,10)
        #df_all=FEsingle.PctChgSumRank(df_all,10)

        
        df_all=FEsingle.MoneyflowallChgRank(df_all,5)
        df_all=FEsingle.MoneyflowsumChgRank(df_all,5)
        df_all=FEsingle.MoneyflowChgRank(df_all,10)
        df_all=FEsingle.AmountChgRank(df_all,10)
        #df_all=FEsingle.AmountChgRank(df_all,40)   

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10','pst_net_mf_amount_rank_5','pst_net_mf_amount_sum_rank_5'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10','pst_net_mf_amount_rank_5','pst_net_mf_amount_sum_rank_5'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10','pst_net_mf_amount_rank_5','pst_net_mf_amount_sum_rank_5'],3)

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','moneyflow','net_mf_amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        #df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)


        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEg30q(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_moneyflow_all=pd.read_csv(DataSetName[3],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_moneyflow_all, how='inner', on=['ts_code','trade_date'])
        
        df_all['moneyflow']=df_all['buy_lg_amount']+df_all['buy_md_amount']+df_all['buy_elg_amount']-df_all['sell_lg_amount']-df_all['sell_md_amount']-df_all['sell_elg_amount']

        df_all.drop(['buy_lg_amount','buy_sm_vol','buy_sm_amount','sell_sm_vol',
                     'sell_sm_amount','buy_md_vol','buy_md_amount','sell_md_vol','sell_md_amount',
                     'buy_lg_vol','sell_lg_vol','sell_lg_amount','buy_elg_vol','buy_elg_amount',
                     'sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.78<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,15,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,15,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#

        df_all=FEsingle.PredictDays(df_all,5)

        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//2


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,10)
        #df_all=FEsingle.PctChgSumRank(df_all,10)

        
        df_all=FEsingle.MoneyflowallChgRank(df_all,5)
        df_all=FEsingle.MoneyflowallChgRank(df_all,20)
        df_all=FEsingle.MoneyflowsumChgRank(df_all,5)
        df_all=FEsingle.MoneyflowsumChgRank(df_all,20)
        df_all=FEsingle.MoneyflowChgRank(df_all,10)
        df_all=FEsingle.AmountChgRank(df_all,10)
        #df_all=FEsingle.AmountChgRank(df_all,40)   

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10','pst_net_mf_amount_rank_5','pst_net_mf_amount_sum_rank_5'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low'],3)

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','moneyflow','net_mf_amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        #df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)


        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEh30f(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.78<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        ##30日最低比值
        #xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        ##bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        ##ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        #df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        #df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        #df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        ##30日最高比值
        #xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        ##bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        ##ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        #df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        #df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        #df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        #20日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(20).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_20min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['20_pct']=(df_all['real_price']-df_all['real_price_20min'])/df_all['real_price_20min']
        df_all['20_pct_rank']=df_all.groupby('trade_date')['20_pct'].rank(pct=True)
        df_all['20_pct_rank']=df_all['20_pct_rank']*10//2

        #20日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(20).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_20max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['20_pct_max']=(df_all['real_price']-df_all['real_price_20max'])/df_all['real_price_20max']
        df_all['20_pct_max_rank']=df_all.groupby('trade_date')['20_pct_max'].rank(pct=True)
        df_all['20_pct_max_rank']=df_all['20_pct_max_rank']*10//2

        #df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)
        df_all.drop(['20_pct','real_price_20min','20_pct_max','real_price_20max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)
        tm6=df_all.groupby('ts_code')['pct_chg'].shift(-6)
        tm7=df_all.groupby('ts_code')['pct_chg'].shift(-7)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm8=df_all.groupby('ts_code')['pct_chg'].shift(-8)
        tm9=df_all.groupby('ts_code')['pct_chg'].shift(-9)
        tm10=df_all.groupby('ts_code')['pct_chg'].shift(-10)

        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)*((100+tm6)/100)*((100+tm7)/100)*((100+tm8)/100)*((100+tm9)/100)*((100+tm10)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        ##10日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        #df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        ##3日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        #df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        ##120日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(120).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_120')

        #df_all['chg_rank_120']=df_all.groupby('trade_date')['chg_rank_120'].rank(pct=True)
        #df_all['chg_rank_120']=df_all['chg_rank_120']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        #df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ###加入前日open
        #df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        #df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        ##df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        #df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ###加入前日open
        #df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        #df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        #df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        #df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)


        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEsingle:
    def CloseWithHighLow(df_all,days,minmax='min'):
        #输入几日和最高或最低返回排名
        #30日最低比值

        stringdisplay=str(days)+'_pct_rank_'+minmax
        if(minmax=='min'):
            xxx=df_all.groupby('ts_code')['real_price'].rolling(days).min().reset_index()
        else:
            xxx=df_all.groupby('ts_code')['real_price'].rolling(days).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)       

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all[stringdisplay]=df_all.groupby('trade_date')['30_pct'].rank(pct=True)

        df_all.drop(['30_pct','real_price_30min'],axis=1,inplace=True)
        df_all=FEsingle.changerank_line(df_all,stringdisplay)

        return df_all,stringdisplay

    def changerank_line(df_all,b):
        df_all[b]=df_all[b]*10//2
        return df_all
    
    def PredictDays(df_all,days):

        ##明日幅度

        tms=[]
        for i in range(days):
            curindex=-i-1
            curtm=df_all.groupby('ts_code')['pct_chg'].shift(curindex)
            tms.append(curtm)

        tmpdf=((100+tms[0])/100)
        for i in range(days):
            if i==0:
                continue
            tmpdf*=(100+tms[i])/100

        df_all['tomorrow_chg']=(tmpdf-1)*100

        #df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1

        return df_all

    def PctChgSumRank(df_all,days):

        bufferbak='_'+str(days)
        stringdisplay='chg_rank_'+str(days)
        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(days).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        df_all[stringdisplay]=df_all.groupby('trade_date')[stringdisplay].rank(pct=True)
        df_all=FEsingle.changerank_line(df_all,stringdisplay)

        return df_all

    def AmountChgRank(df_all,days):

        bufferbak='_'+str(days)
        amountstring='amount'+bufferbak
        stringdisplay='pst_amount_rank_'+str(days)

        #均量计算
        xxx=df_all.groupby('ts_code')['amount'].rolling(days).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all[amountstring]
        df_all.drop([amountstring],axis=1,inplace=True)
        #当日量排名
        df_all[stringdisplay]=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all=FEsingle.changerank_line(df_all,stringdisplay)

        df_all.drop(['pst_amount'],axis=1,inplace=True)
        return df_all

    def MoneyflowChgRank(df_all,days):

        bufferbak='_'+str(days)
        moneyflowstring='moneyflow'+bufferbak
        stringdisplay='pst_moneyflow_rank_'+str(days)

        #均量计算
        xxx=df_all.groupby('ts_code')['moneyflow'].rolling(days).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        #当日量占比
        df_all['pst_moneyflow']=df_all['moneyflow']/df_all[moneyflowstring]
        df_all.drop([moneyflowstring],axis=1,inplace=True)
        #当日量排名
        df_all[stringdisplay]=df_all.groupby('trade_date')['pst_moneyflow'].rank(pct=True)
        df_all=FEsingle.changerank_line(df_all,stringdisplay)

        df_all.drop(['pst_moneyflow'],axis=1,inplace=True)
        return df_all

    def MoneyflowallChgRank(df_all,days):

        bufferbak='_'+str(days)
        net_mf_amountstring='net_mf_amount'+bufferbak
        stringdisplay='pst_net_mf_amount_rank_'+str(days)

        #均量计算
        xxx=df_all.groupby('ts_code')['net_mf_amount'].rolling(days).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        #当日量占比
        df_all['pst_net_mf_amount']=df_all['net_mf_amount']/df_all[net_mf_amountstring]
        df_all.drop([net_mf_amountstring],axis=1,inplace=True)
        #当日量排名
        df_all[stringdisplay]=df_all.groupby('trade_date')['pst_net_mf_amount'].rank(pct=True)
        df_all=FEsingle.changerank_line(df_all,stringdisplay)

        df_all.drop(['pst_net_mf_amount'],axis=1,inplace=True)
        return df_all

    def MoneyflowsumChgRank(df_all,days):

        bufferbak='_'+str(days)
        net_mf_amountstring='net_mf_amount'+bufferbak
        stringdisplay='pst_net_mf_amount_sum_rank_'+str(days)

        #总量计算
        xxx=df_all.groupby('ts_code')['net_mf_amount'].rolling(days).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        #当日量排名
        df_all[stringdisplay]=df_all.groupby('trade_date')[net_mf_amountstring].rank(pct=True)
        df_all=FEsingle.changerank_line(df_all,stringdisplay)

        df_all.drop([net_mf_amountstring],axis=1,inplace=True)
        return df_all

    def OldFeaturesRank(df_all,features,daybak):
      
        for curfeature in features:
            curstring='yesterday_'+str(daybak)+curfeature
            df_all[curstring]=df_all.groupby('ts_code')[curfeature].shift(daybak)

        return df_all