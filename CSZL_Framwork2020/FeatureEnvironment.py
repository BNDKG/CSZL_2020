#coding=utf-8
import pandas as pd
import numpy as np
import sys
import os
from sklearn import preprocessing
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

class FEg30eom(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#


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

        #删除股价过低的票
        #df_all=df_all[df_all['close']>5]
        #df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all=FEsingle.PredictDays(df_all,5)
        df_all.drop(['pct_chg'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)


        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):
        #新模型预定版本

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom1231a(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*19.9//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#


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
            df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)


        df_all.drop(['pre_close','adj_factor','real_price','total_mv','pb','circ_mv'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDays(df_all,5)
        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        #df_all=df_all[df_all['close']>3]
        df_all=df_all[df_all['total_mv_rank']>18]
        #df_all=df_all[df_all['total_mv_rank']<7]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['circ_mv_pct']>3]
        #df_all=df_all[df_all['ps_ttm']>3]
        #df_all=df_all[df_all['pb_rank']>3]

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]
        #'tomorrow_chg'
        df_all.drop(['high_stop','amount','close'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)
        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):
        #新模型预定版本

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom0110onlinee(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        #df_all['real_open']=df_all['adj_factor']*df_all['open']
        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*19.9//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        #df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        #df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2

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
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDaysStart(df_all,3)
        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        #df_all=df_all[df_all['close']>3]
        df_all=df_all[df_all['total_mv_rank']>14]
        #df_all=df_all[df_all['total_mv_rank']<12]
        #df_all=df_all[df_all['amount']>200000]
        #df_all=df_all[df_all['circ_mv_pct']>3]
        #df_all=df_all[df_all['ps_ttm']>3]
        #df_all=df_all[df_all['pb_rank']>3]

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]
        #'tomorrow_chg'
        df_all.drop(['high_stop','amount','close','real_price'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)
        #df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):
        #新模型预定版本

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        print(df_all)

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        df_all['ts_code'] = df_all['ts_code'].astype('str') #将原本的int数据类型转换为文本

        df_all['ts_code']  = df_all['ts_code'].str.zfill(6) #用的时候必须加上.str前缀

        print(df_all)
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*19.9//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        #df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        #df_all['ps_ttm']=df_all['ps_ttm']*10//1


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
        #df_all['chg_rank']=df_all['chg_rank']*10//2

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
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除市值过低的票
        #df_all=df_all[df_all['close']>5]
        #df_all=df_all[df_all['total_mv_rank']<5]
        #df_all=df_all[df_all['amount']>15000]
        df_all=df_all[df_all['total_mv_rank']>14]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom0110onlinef(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        #df_all['real_open']=df_all['adj_factor']*df_all['open']
        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*19.9//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        #df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        #df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2

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
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDaysStart(df_all,5)
        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['total_mv_rank']>14]
        df_all=df_all[df_all['total_mv_rank']<12]
        #df_all=df_all[df_all['total_mv_rank']>7]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['circ_mv_pct']>3]
        #df_all=df_all[df_all['ps_ttm']>3]
        #df_all=df_all[df_all['pb_rank']>3]

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]
        #'tomorrow_chg'
        df_all.drop(['high_stop','amount','close','real_price'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)
        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):
        #新模型预定版本

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        print(df_all)

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        df_all['ts_code'] = df_all['ts_code'].astype('str') #将原本的int数据类型转换为文本

        df_all['ts_code']  = df_all['ts_code'].str.zfill(6) #用的时候必须加上.str前缀

        print(df_all)
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*19.9//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        #df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        #df_all['ps_ttm']=df_all['ps_ttm']*10//1


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
        #df_all['chg_rank']=df_all['chg_rank']*10//2

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
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom0110onlinej(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        #df_all['real_open']=df_all['adj_factor']*df_all['open']
        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*19.9//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        #df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        #df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)

        df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDaysStart(df_all,10)
        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['total_mv_rank']>14]
        #df_all=df_all[df_all['total_mv_rank']>18]
        #df_all=df_all[df_all['total_mv_rank']>2]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['circ_mv_pct']>3]
        #df_all=df_all[df_all['ps_ttm']>3]
        #df_all=df_all[df_all['pb_rank']>3]

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]
        #'tomorrow_chg'
        df_all.drop(['high_stop','amount','close','real_price'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)
        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):
        #新模型预定版本

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        print(df_all)

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        df_all['ts_code'] = df_all['ts_code'].astype('str') #将原本的int数据类型转换为文本

        df_all['ts_code']  = df_all['ts_code'].str.zfill(6) #用的时候必须加上.str前缀

        print(df_all)
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*19.9//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        #df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        #df_all['ps_ttm']=df_all['ps_ttm']*10//1


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
        #df_all['chg_rank']=df_all['chg_rank']*10//2

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
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom0110onlinep(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        #df_all['real_open']=df_all['adj_factor']*df_all['open']
        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*19.9//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all.loc[df_all['pe']<0,'pe']=100
        df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        df_all['pe_rank']=df_all.groupby('ts_code')['pe_rank'].shift(1)

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        #df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        #df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)

        df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDaysStart(df_all,2)
        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        #df_all=df_all[df_all['total_mv_rank']<17]
        #df_all=df_all[df_all['total_mv_rank']>5]
        #df_all=df_all[df_all['total_mv_rank']>2]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['circ_mv_pct']>3]
        #df_all=df_all[df_all['ps_ttm']>3]
        #df_all=df_all[df_all['pb_rank']>3]

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]
        #'tomorrow_chg'
        df_all.drop(['high_stop','amount','close','real_price'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)
        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):
        #新模型预定版本

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        print(df_all)

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        df_all['ts_code'] = df_all['ts_code'].astype('str') #将原本的int数据类型转换为文本

        df_all['ts_code']  = df_all['ts_code'].str.zfill(6) #用的时候必须加上.str前缀

        print(df_all)
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*19.9//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        #df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        #df_all['ps_ttm']=df_all['ps_ttm']*10//1


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
        #df_all['chg_rank']=df_all['chg_rank']*10//2

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
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom0110onlines(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        #df_all['real_open']=df_all['adj_factor']*df_all['open']
        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*19.9//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all.loc[df_all['pe']<0,'pe']=100
        df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        df_all['pe_rank']=df_all.groupby('ts_code')['pe_rank'].shift(1)

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        #df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        #df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow_notrank(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow_notrank(df_all,8,'min')
        df_all,_=FEsingle.CloseWithHighLow_notrank(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow_notrank(df_all,8,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2

        df_all=FEsingle.PctChgSumRank_notrank(df_all,3)
        df_all=FEsingle.PctChgSumRank_notrank(df_all,6)
        df_all=FEsingle.PctChgSumRank_notrank(df_all,12)

        df_all=FEsingle.AmountChg_notrank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_notrank_12'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_notrank_12'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_notrank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDaysStart(df_all,5)
        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        #df_all=df_all[df_all['total_mv_rank']<17]
        #df_all=df_all[df_all['total_mv_rank']>5]
        #df_all=df_all[df_all['total_mv_rank']>2]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['circ_mv_pct']>3]
        #df_all=df_all[df_all['ps_ttm']>3]
        #df_all=df_all[df_all['pb_rank']>3]

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]
        #'tomorrow_chg'
        df_all.drop(['high_stop','amount','close','real_price'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)
        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):
        #新模型预定版本

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        print(df_all)

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        df_all['ts_code'] = df_all['ts_code'].astype('str') #将原本的int数据类型转换为文本

        df_all['ts_code']  = df_all['ts_code'].str.zfill(6) #用的时候必须加上.str前缀

        print(df_all)
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*19.9//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        #df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        #df_all['ps_ttm']=df_all['ps_ttm']*10//1


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
        #df_all['chg_rank']=df_all['chg_rank']*10//2

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
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class trend_following(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        #df_all.drop(['turnover_rate','volume_ratio','dv_ttm'],axis=1,inplace=True)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        #df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        longline=60
        bufferbak='_'+str(longline)
        #均量计算
        xxx=df_all.groupby('ts_code')['real_price'].rolling(longline).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        shortline=5
        bufferbak='_'+str(shortline)
        #均量计算
        xxx=df_all.groupby('ts_code')['real_price'].rolling(shortline).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        df_all.dropna(axis=0,how='any',inplace=True)
        print(df_all)
        #寻找unique
        codelistbuffer=df_all['ts_code']
        codelistbuffer=codelistbuffer.unique()

        codelist=codelistbuffer.tolist()

        cutlimit=-0.1

        df_ana = pd.DataFrame(columns = ["code", "data", "differ"])
        ct=0

        for curcode in codelist:
            cur_code_df=df_all[df_all['ts_code']==curcode]
            count_flag=0
            startprice=0
            stopprice=0
            changesum=1
            
            start0_price=cur_code_df.loc[cur_code_df.index[0]].values[12]
            for indexs in cur_code_df.index:
                cur_code_day=cur_code_df.loc[indexs].values[1]
                cur_code_now=cur_code_df.loc[indexs].values[12]
                cur_code_avg_short=cur_code_df.loc[indexs].values[14]
                cur_code_avg_long=cur_code_df.loc[indexs].values[13]
                if(cur_code_avg_short>cur_code_avg_long):
                    if(count_flag==0):
                        count_flag=1
                        startprice=cur_code_now

                        #print(cur_code_day,end='')
                        #print('   ',end='')
                        ##print(cur_code_day)
                        #print(startprice,end='')
                        #print('   ',end='')
                        ##print(cur_code_day)
                        #print('buy')
              

                if(count_flag==1):
                    #计算止损
                    nowcut=(cur_code_now-startprice)/startprice
                    if(nowcut<cutlimit):
                        count_flag=0
                        differ=nowcut
                        changesum=changesum*(1+differ)
                        #print(cur_code_day,end='')
                        #print('   ',end='')
                        #print(cur_code_now,end='')
                        #print('   ',end='')
                        #print('cutsell')
                        df_ana.loc[df_ana.shape[0]+1] = {'code':curcode,'data':cur_code_day,'differ':differ}
                        continue

                    if(cur_code_avg_short<cur_code_avg_long):
                        count_flag=0
                        #stopprice=cur_code_now
                        #print(cur_code_day,end='')
                        #print('   ',end='')
                        #print(cur_code_now,end='')
                        #print('   ',end='')
                        #print('sell')
                        differ=(cur_code_now-startprice)/startprice
                        changesum=changesum*(1+differ)
                        df_ana.loc[df_ana.shape[0]+1] = {'code':curcode,'data':cur_code_day,'differ':differ}

                #print(cur_code_df.loc[indexs].values[0:-1])

            if(count_flag==1):
                differ=(cur_code_now-startprice)/startprice
                changesum=changesum*(1+differ)
                df_ana.loc[df_ana.shape[0]+1] = {'code':curcode,'data':cur_code_day,'differ':differ}

            #if(ct%100==0):
            #    #df_ana.to_csv('save10_2avg.csv')
            #    print(ct)
            ct+=1
            stopprice=cur_code_df.loc[cur_code_df.index[-1]].values[12]

            pctnostra=stopprice/start0_price

            print(curcode,end='')
            print('   ',end='')
            print(pctnostra,end='')
            print('   ',end='')
            print(changesum)
            #print(cur_code_df)
            dddddddd=1

        df_ana.to_csv('save10_2avg.csv')
        #df_all.to_csv('read5mean.csv')

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):
        #新模型预定版本

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        print(df_all)

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        df_all['ts_code'] = df_all['ts_code'].astype('str') #将原本的int数据类型转换为文本

        df_all['ts_code']  = df_all['ts_code'].str.zfill(6) #用的时候必须加上.str前缀

        print(df_all)
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*19.9//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        #df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        #df_all['ps_ttm']=df_all['ps_ttm']*10//1


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
        #df_all['chg_rank']=df_all['chg_rank']*10//2

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
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eomsave(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

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

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom3(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        #df_all['class1']=0
        #df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        #df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        #df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

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

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom2(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

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

        #删除股价过低的票
        #df_all=df_all[df_all['close']>5]
        #df_all=df_all[df_all['total_mv_rank']<5]
        #df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom_start2(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        df_all['real_open']=df_all['adj_factor']*df_all['open']

        df_all=FEsingle.PredictDaysStart(df_all,5)
        print(df_all)
        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#



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

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','real_open'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom_start3(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        df_all['real_open']=df_all['adj_factor']*df_all['open']

        df_all=FEsingle.PredictDaysStart(df_all,5)
        print(df_all)
        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow_realrank(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow_realrank(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow_realrank(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')
        df_all,_=FEsingle.CloseWithHighLow_realrank(df_all,10,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#



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

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','real_open'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom_start1213a(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        df_all['real_open']=df_all['adj_factor']*df_all['open']

        df_all=FEsingle.PredictDaysStartreal(df_all,5)
        print(df_all)
        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow_realrank(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow_realrank(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow_realrank(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')
        df_all,_=FEsingle.CloseWithHighLow_realrank(df_all,10,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#



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

        #删除股价过低的票
        #df_all=df_all[df_all['close']>5]
        #df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','real_open'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom_randomstart2(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        df_all['real_open']=df_all['adj_factor']*df_all['open']


        #df_all=FEsingle.random_mix(df_all)
        df_all=FEsingle.PredictDaysStart(df_all,5)
        print(df_all)
        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#



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

        mixlist=['circ_mv_pct','30_pct_rank_min','10_pct_rank_min','30_pct_rank_max',
                 '10_pct_rank_max','chg_rank','chg_rank_3','chg_rank_6','chg_rank_10','pst_amount_rank_10']

        length=df_all.shape[0]
        for curc in mixlist:
            data1=np.random.randint(-1,2,size=length)
            df_all[curc]=df_all[curc]+data1
        

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','real_open'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom_test1(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

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

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']>1]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom_basic(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)
        df_basic=pd.read_csv(DataSetName[5],index_col=0,header=0)

        df_basic.drop(['symbol','name','list_date'],axis=1,inplace=True)

        le = preprocessing.LabelEncoder()

        le.fit(df_basic['area'].values)
        sefs=le.transform(df_basic['area'].values)
        le.fit(df_basic['market'].values)
        sefs2=le.transform(df_basic['market'].values)

        df_basic['area2']=sefs
        df_basic['market2']=sefs2

        df_basic.drop(['area'],axis=1,inplace=True)
        df_basic.drop(['market'],axis=1,inplace=True)
        print(df_basic)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

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

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=pd.merge(df_all, df_basic, how='left', on=['ts_code'])
        df_all.fillna(value=0, inplace=True)
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):
        #新模型预定版本

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom_all(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

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

        #删除股价过低的票
        #df_all=df_all[df_all['close']>5]
        #df_all=df_all[df_all['total_mv_rank']<5]
        #df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom_reg(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#

        df_all=FEsingle.PredictDays_reg(df_all,5)

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

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom_reg_concept3(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)
        df_basic=pd.read_csv(DataSetName[5],index_col=0,header=0)
        #df_conceptlist=pd.read_csv(DataSetName[6],index_col=0,header=0)
        #df_concept=pd.read_csv(DataSetName[7],index_col=0,header=0)
        df_conceptmixed=pd.read_csv(DataSetName[8],index_col=0,header=0)

        df_basic.drop(['symbol','name','list_date'],axis=1,inplace=True)

        le = preprocessing.LabelEncoder()

        le.fit(df_basic['area'].values)
        sefs=le.transform(df_basic['area'].values)
        le.fit(df_basic['market'].values)
        sefs2=le.transform(df_basic['market'].values)

        df_basic['area2']=sefs
        df_basic['market2']=sefs2

        df_basic.drop(['area'],axis=1,inplace=True)
        df_basic.drop(['market'],axis=1,inplace=True)
        print(df_basic)
        print(df_conceptmixed)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#

        df_all=FEsingle.PredictDays_reg(df_all,5)

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

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['st_or_otherwrong']==1]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=pd.merge(df_all, df_basic, how='left', on=['ts_code'])
        df_all.fillna(value=0, inplace=True)
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):
        #新模型预定版本

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom_reg_mini(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#

        df_all=FEsingle.PredictDays_reg(df_all,5)

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

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        #df_all=df_all[df_all['close']>5]
        #df_all=df_all[df_all['total_mv_rank']<5]
        #df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom_reg_1(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#

        df_all=FEsingle.PredictDays(df_all,1)

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

        #删除股价过低的票
        #df_all=df_all[df_all['close']>5]
        #df_all=df_all[df_all['total_mv_rank']<5]
        #df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom_5all(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

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

        #删除股价过低的票
        #df_all=df_all[df_all['close']>5]
        #df_all=df_all[df_all['total_mv_rank']<5]
        #df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom_no300(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all=df_all[df_all['ts_code'].str.startswith('300')==False]
        df_all['class1']=0
        #df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

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

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eom_300(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all=df_all[df_all['ts_code'].str.startswith('300')==True]
        #df_all['class1']=0
        #df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        #df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        #df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

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

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FE_cat_20(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.5<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #        #加入gap_day特征
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

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        #df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#

        df_all=FEsingle.PredictDays(df_all,5)

        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2

        #df_all=FEsingle.PctChgSumRank(df_all,3)
        #df_all=FEsingle.PctChgSumRank(df_all,6)
        #df_all=FEsingle.PctChgSumRank(df_all,10)
        #df_all=FEsingle.PctChgSumRank(df_all,30)

        #df_all=FEsingle.AmountChgRank(df_all,10)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low'],1)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

class FEg30eomc(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.78<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

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

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        #df_all=df_all[df_all['total_mv_rank']>5]
        df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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
        df_all['30_pct']=(100*(df_all['real_price']+0.001-df_all['real_price_30min']))/df_all['real_price_30min']
        df_all[stringdisplay]=df_all.groupby('trade_date')['30_pct'].rank(pct=True)

        df_all.drop(['30_pct','real_price_30min'],axis=1,inplace=True)
        #df_all=FEsingle.changerank_line(df_all,stringdisplay)

        return df_all,stringdisplay

    def CloseWithHighLow_notrank(df_all,days,minmax='min'):
        #输入几日和最高或最低返回实际值

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
        df_all[stringdisplay]=(10*(df_all['real_price']-df_all['real_price_30min']))/df_all['real_price_30min']

        df_all.drop(['real_price_30min'],axis=1,inplace=True)
        df_all=FEsingle.changerank_line(df_all,stringdisplay)

        return df_all,stringdisplay

    def CloseWithHighLow_realrank(df_all,days,minmax='min'):
        #输入几日和最高或最低返回排名
        #30日最低比值

        stringdisplay=str(days)+'_pct_realrank_'+minmax
        if(minmax=='min'):
            xxx=df_all.groupby('ts_code')['real_price'].rolling(days).min().reset_index()
        else:
            xxx=df_all.groupby('ts_code')['real_price'].rolling(days).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)       

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(100*(df_all['real_price']-df_all['real_price_30min']))/df_all['real_price_30min']
        df_all[stringdisplay]=df_all['30_pct'].rank(pct=True)

        df_all.drop(['30_pct','real_price_30min'],axis=1,inplace=True)
        df_all=FEsingle.changerank_line(df_all,stringdisplay)

        return df_all,stringdisplay

    def changerank_line(df_all,b):
        df_all[b]=df_all[b]*9.9//2
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

    def PredictDaysStart(df_all,days):

        ##明日幅度


        nextstart=df_all.groupby('ts_code')['real_price'].shift(0)
        nextnstart=df_all.groupby('ts_code')['real_price'].shift(0-days)

        #df_all['real_open2']=nextnstart
        #df_all['real_open1']=nextstart
        #df_all['real_open0']=df_all['real_open']

        df_all['tomorrow_chg']=((nextnstart-nextstart)/nextstart)*100

        #df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1

        return df_all

    def PredictDaysReal(df_all,days):

        ##明日幅度


        nextstart=df_all.groupby('ts_code')['real_price'].shift(0)
        nextnstart=df_all.groupby('ts_code')['real_price'].shift(0-days)

        #df_all['real_open2']=nextnstart
        #df_all['real_open1']=nextstart
        #df_all['real_open0']=df_all['real_open']

        df_all['tomorrow_chg']=((nextnstart-nextstart)/nextstart)*100

        #df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        #df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=0
        #df_all.loc[df_all['tomorrow_chg']>-4,'tomorrow_chg_rank']=1
        #df_all.loc[df_all['tomorrow_chg']>-2.3,'tomorrow_chg_rank']=2
        #df_all.loc[df_all['tomorrow_chg']>-1.4,'tomorrow_chg_rank']=3
        #df_all.loc[df_all['tomorrow_chg']>-0.7,'tomorrow_chg_rank']=4
        #df_all.loc[df_all['tomorrow_chg']>0,'tomorrow_chg_rank']=5
        #df_all.loc[df_all['tomorrow_chg']>0.7,'tomorrow_chg_rank']=6
        #df_all.loc[df_all['tomorrow_chg']>1.4,'tomorrow_chg_rank']=7
        #df_all.loc[df_all['tomorrow_chg']>2.3,'tomorrow_chg_rank']=8
        #df_all.loc[df_all['tomorrow_chg']>4,'tomorrow_chg_rank']=9
        df_all.loc[df_all['tomorrow_chg']>-8,'tomorrow_chg_rank']=1
        df_all.loc[df_all['tomorrow_chg']>-4.6,'tomorrow_chg_rank']=2
        df_all.loc[df_all['tomorrow_chg']>-2.8,'tomorrow_chg_rank']=3
        df_all.loc[df_all['tomorrow_chg']>-1.4,'tomorrow_chg_rank']=4
        df_all.loc[df_all['tomorrow_chg']>0,'tomorrow_chg_rank']=5
        df_all.loc[df_all['tomorrow_chg']>1.4,'tomorrow_chg_rank']=6
        df_all.loc[df_all['tomorrow_chg']>2.8,'tomorrow_chg_rank']=7
        df_all.loc[df_all['tomorrow_chg']>4.6,'tomorrow_chg_rank']=8
        df_all.loc[df_all['tomorrow_chg']>8,'tomorrow_chg_rank']=9
        return df_all

    def PredictDaysStartreal(df_all,days):

        ##明日幅度


        nextstart=df_all.groupby('ts_code')['real_open'].shift(-1)
        nextnstart=df_all.groupby('ts_code')['real_open'].shift(-5)

        #df_all['real_open2']=nextnstart
        #df_all['real_open1']=nextstart
        #df_all['real_open0']=df_all['real_open']

        df_all['tomorrow_chg']=(nextnstart-nextstart)/nextstart

        #df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg']*10//1

        return df_all

    def random_mix(df_all):

        ##明日幅度

        length=df_all.shape[0]
        data1=np.random.randint(-1,2,size=length)
        data2=pd.DataFrame(data1)

        df_all['rdtest']=1

        df_all['rdtest']=df_all['rdtest']+data1

        print(df_all)
        nextstart=df_all.groupby('ts_code')['real_open'].shift(-1)
        nextnstart=df_all.groupby('ts_code')['real_open'].shift(-5)

        #df_all['real_open2']=nextnstart
        #df_all['real_open1']=nextstart
        #df_all['real_open0']=df_all['real_open']

        df_all['tomorrow_chg']=(nextnstart-nextstart)/nextstart

        #df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1

        return df_all

    def PredictDays_reg(df_all,days):

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
        #df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1

        return df_all

    def PredictDays_notrank(df_all,days):

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

        #明日排名
        df_all['tomorrow_chg_rank']=0
        df_all.loc[df_all['tomorrow_chg']>-10,'tomorrow_chg_rank']=1
        df_all.loc[df_all['tomorrow_chg']>-6,'tomorrow_chg_rank']=2
        df_all.loc[df_all['tomorrow_chg']>-3,'tomorrow_chg_rank']=3
        df_all.loc[df_all['tomorrow_chg']>-1,'tomorrow_chg_rank']=4
        df_all.loc[df_all['tomorrow_chg']>0,'tomorrow_chg_rank']=5
        df_all.loc[df_all['tomorrow_chg']>1,'tomorrow_chg_rank']=6
        df_all.loc[df_all['tomorrow_chg']>3,'tomorrow_chg_rank']=7
        df_all.loc[df_all['tomorrow_chg']>6,'tomorrow_chg_rank']=8
        df_all.loc[df_all['tomorrow_chg']>10,'tomorrow_chg_rank']=9



        return df_all

    def PctChgSumRank(df_all,days):

        bufferbak='_'+str(days)
        stringdisplay='chg_rank_'+str(days)

        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(days).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        df_all[stringdisplay]=df_all.groupby('trade_date')[stringdisplay].rank(pct=True)
        #df_all=FEsingle.changerank_line(df_all,stringdisplay)

        return df_all

    def PctChgSumRank_notrank(df_all,days):

        bufferbak='_'+str(days)
        stringdisplay='chg_rank_'+str(days)

        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(days).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        return df_all

    def PctChgSumRank_Common(df_all,days,namechg='high'):

        bufferbak='_'+str(days)
        stringdisplay=namechg+str(days)
        #6日
        xxx=df_all.groupby('ts_code')[namechg].rolling(days).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        df_all[stringdisplay]=df_all.groupby('trade_date')[stringdisplay].rank(pct=True)
        #df_all=FEsingle.changerank_line(df_all,stringdisplay)

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
        #df_all=FEsingle.changerank_line(df_all,stringdisplay)

        df_all.drop(['pst_amount'],axis=1,inplace=True)
        return df_all

    def AmountChg_notrank(df_all,days):

        bufferbak='_'+str(days)
        amountstring='amount'+bufferbak
        stringdisplay='pst_amount_notrank_'+str(days)

        #均量计算
        xxx=df_all.groupby('ts_code')['amount'].rolling(days).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        #当日量占比
        df_all[stringdisplay]=df_all['amount']/df_all[amountstring]
        df_all.drop([amountstring],axis=1,inplace=True)

        return df_all

    def MoneyflowChgRank(df_all,days,shiftday=1):

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
        #df_all=FEsingle.changerank_line(df_all,stringdisplay)
        #当日数据无法获取只能获取上一日数据
        df_all[stringdisplay]=df_all.groupby('ts_code')[stringdisplay].shift(shiftday)
        df_all.drop(['pst_moneyflow'],axis=1,inplace=True)
        return df_all

    def MoneyflowallChgRank(df_all,days,shiftday=1):

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
        #df_all=FEsingle.changerank_line(df_all,stringdisplay)

        #当日数据无法获取只能获取上一日数据
        df_all[stringdisplay]=df_all.groupby('ts_code')[stringdisplay].shift(shiftday)
        df_all.drop(['pst_net_mf_amount'],axis=1,inplace=True)
        return df_all

    def MoneyflowsumChgRank(df_all,days,shiftday=1):

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
        #df_all=FEsingle.changerank_line(df_all,stringdisplay)

        #当日数据无法获取只能获取上一日数据
        df_all[stringdisplay]=df_all.groupby('ts_code')[stringdisplay].shift(shiftday)
        df_all.drop([net_mf_amountstring],axis=1,inplace=True)
        return df_all

    def OldFeaturesRank(df_all,features,daybak):
      
        for curfeature in features:
            curstring='yesterday_'+str(daybak)+curfeature
            df_all[curstring]=df_all.groupby('ts_code')[curfeature].shift(daybak)

        return df_all

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

class FE3_test5(FEbase):
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

class FE_banana_9(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.5<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        ##排除科创版
        #print(df_all)
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        #df_all['class1']=0
        #df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        #df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        #df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
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

        #===================================================================================================================================#

        #df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        #df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        #df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        #df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        #df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        #df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        #df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        #df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        #df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'min')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,10,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,30,'max')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,10,'max')

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#

        df_all=FEsingle.PredictDays(df_all,1)

        #是否停
        df_all['high_stop']=0
        #df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[df_all['pct_chg']>4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.4) & (4.6<df_all['pct_chg']),'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1

        #df_all=FEsingle.PctChgSumRank(df_all,1)
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
            df_all[curc]=df_all[curc]*10//1


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        #df_all=df_all[df_all['close']>5]
        #df_all=df_all[df_all['total_mv_rank']<5]
        #df_all=df_all[df_all['amount']>15000]
        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','ps_ttm'],axis=1,inplace=True)
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
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        #这里打一个问号
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 
        ##排除科创版
        #print(df_all)
        df_all[["ts_code"]]=df_all[["ts_code"]].astype(str)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)

        #===================================================================================================================================#

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        df_all['total_mv_rank']=df_all['total_mv_rank']*10//1

        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        df_all['ps_ttm']=df_all['ps_ttm']*10//1


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

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_10'],3)

        #删除股价过低的票
        df_all=df_all[df_all['close']>5]
        df_all=df_all[df_all['total_mv_rank']<5]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv'],axis=1,inplace=True)

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

