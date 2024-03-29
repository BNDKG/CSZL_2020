#coding=utf-8
import pandas as pd
import numpy as np
import sys
import os
from sklearn import preprocessing
import datetime
import scipy as sc

from sklearn.preprocessing import MinMaxScaler,StandardScaler
from sklearn.externals import joblib
#import joblib

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

class FEg30eom0110network(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        intflag=True

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
        if(intflag):
            df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        if(intflag):
            df_all['circ_mv_pct']=df_all['circ_mv_pct']*10//1
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        if(intflag):
            df_all['ps_ttm']=df_all['ps_ttm']*10//1
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min',True)
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'min',True)
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max',True)
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'max',True)

        df_all,_=FEsingle.HighLowRange(df_all,8,True)
        df_all,_=FEsingle.HighLowRange(df_all,25,True)   

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        if(intflag):
            df_all['chg_rank']=df_all['chg_rank']*10//2

        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)
        if(intflag):
            df_all['pct_chg_abs_rank']=df_all['pct_chg_abs_rank']*10//2
        
        df_all=FEsingle.PctChgAbsSumRank(df_all,6,True)
        df_all=FEsingle.PctChgSumRank(df_all,3,True)
        df_all=FEsingle.PctChgSumRank(df_all,6,True)
        df_all=FEsingle.PctChgSumRank(df_all,12,True)

        df_all=FEsingle.AmountChgRank(df_all,12,True)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            if(intflag):
                df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],2)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDaysTrend(df_all,5)
        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
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

class FEg30eom0110onlinew6d(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_money_all=pd.read_csv(DataSetName[3],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)



        print(df_money_all)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='inner', on=['ts_code','trade_date'])

        df_all['sm_amount_pos']=df_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_all['lg_amount_pos']=df_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_all['net_mf_amount_pos']=df_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        df_all['sm_amount_pos']=df_all.groupby('ts_code')['sm_amount_pos'].shift(1)
        df_all['lg_amount_pos']=df_all.groupby('ts_code')['lg_amount_pos'].shift(1)
        df_all['net_mf_amount_pos']=df_all.groupby('ts_code')['net_mf_amount_pos'].shift(1)

        df_all['sm_amount']=df_all.groupby('ts_code')['sm_amount'].shift(1)
        df_all['lg_amount']=df_all.groupby('ts_code')['lg_amount'].shift(1)
        df_all['net_mf_amount']=df_all.groupby('ts_code')['net_mf_amount'].shift(1)

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
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        #df_all['real_open']=df_all['adj_factor']*df_all['open']
        #===================================================================================================================================#
        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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

        df_all,_=FEsingle.HighLowRange(df_all,8)
        df_all,_=FEsingle.HighLowRange(df_all,25)   

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

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

        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        
        #df_all=FEsingle.PctChgAbsSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)


        df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12','real_price_pos'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDaysTrend(df_all,5)


        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
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

class FE_a23(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_money_all=pd.read_csv(DataSetName[3],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos']
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos']
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos']

        #df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        #df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        #df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)

        df_money_all=FEsingle.InputChgSum(df_money_all,5,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,12,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,25,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'net_mf_amount')

        print(df_money_all)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        df_all['dayofweek']=pd.to_datetime(df_all['trade_date'],format='%Y%m%d')
        df_all['dayofweek']=df_all['dayofweek'].dt.dayofweek

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
        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'max')

        df_all,_=FEsingle.HighLowRange(df_all,5)
        df_all,_=FEsingle.HighLowRange(df_all,12)
        df_all,_=FEsingle.HighLowRange(df_all,25)    

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

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

        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        
        df_all=FEsingle.PctChgAbsSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)
        df_all=FEsingle.PctChgSumRank(df_all,24)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)
        df_all=FEsingle.PctChgSum(df_all,24)


        #df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','real_price_pos'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDaysTrend(df_all,5)


        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>2]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
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
        df_money_all=pd.read_csv('real_moneyflow_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos'].shift(1)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos'].shift(1)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos'].shift(1)

        df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)


        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='left', on=['ts_code','trade_date'])
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

        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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


        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'max')

        df_all,_=FEsingle.HighLowRange(df_all,8)
        df_all,_=FEsingle.HighLowRange(df_all,25)  
        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2
        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)

        df_all=FEsingle.AmountChgRank(df_all,12)

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12','real_price_pos'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['chg_rank']>0.7]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)

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

class FE_a29(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_money_all=pd.read_csv(DataSetName[3],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos']
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos']
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos']

        #df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        #df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        #df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)

        df_money_all=FEsingle.InputChgSum(df_money_all,5,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,12,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,25,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'net_mf_amount')

        #df_money_all['sm_amount_25_diff']=df_money_all['sm_amount_25']-df_money_all['sm_amount_12']
        #df_money_all['sm_amount_12_diff']=df_money_all['sm_amount_12']-df_money_all['sm_amount_5']

        #df_money_all['lg_amount_25_diff']=df_money_all['lg_amount_25']-df_money_all['lg_amount_12']
        #df_money_all['lg_amount_12_diff']=df_money_all['lg_amount_12']-df_money_all['lg_amount_5']

        #df_money_all['net_mf_amount_25_diff']=df_money_all['net_mf_amount_25']-df_money_all['net_mf_amount_12']
        #df_money_all['net_mf_amount_12_diff']=df_money_all['net_mf_amount_12']-df_money_all['net_mf_amount_5']


        print(df_money_all)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        df_all['dayofweek']=pd.to_datetime(df_all['trade_date'],format='%Y%m%d')
        df_all['dayofweek']=df_all['dayofweek'].dt.dayofweek

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
        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'max')

        df_all,_=FEsingle.HighLowRange(df_all,5)
        df_all,_=FEsingle.HighLowRange(df_all,12)
        df_all,_=FEsingle.HighLowRange(df_all,25)    

        df_all['25_pct_rank_min_diff']=df_all['25_pct_rank_min']-df_all['12_pct_rank_min']
        df_all['12_pct_rank_min_diff']=df_all['12_pct_rank_min']-df_all['5_pct_rank_min']

        df_all['25_pct_rank_max_diff']=df_all['25_pct_rank_max']-df_all['12_pct_rank_max']
        df_all['12_pct_rank_max_diff']=df_all['12_pct_rank_max']-df_all['5_pct_rank_max']

        df_all['25_pct_Rangerank_diff']=df_all['25_pct_Rangerank']-df_all['12_pct_Rangerank']
        df_all['12_pct_Rangerank_diff']=df_all['12_pct_Rangerank']-df_all['5_pct_Rangerank']

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

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

        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        
        df_all=FEsingle.PctChgAbsSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)
        df_all=FEsingle.PctChgSumRank(df_all,24)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)
        df_all=FEsingle.PctChgSum(df_all,24)

        df_all['chg_rank_24_diff']=df_all['chg_rank_24']-df_all['chg_rank_12']
        df_all['chg_rank_12_diff']=df_all['chg_rank_12']-df_all['chg_rank_6']
        df_all['chg_rank_6_diff']=df_all['chg_rank_6']-df_all['chg_rank_3']

        df_all['pct_chg_24_diff']=df_all['pct_chg_24']-df_all['pct_chg_12']
        df_all['pct_chg_12_diff']=df_all['pct_chg_12']-df_all['pct_chg_6']
        df_all['pct_chg_6_diff']=df_all['pct_chg_6']-df_all['pct_chg_3']


        #df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','real_price_pos'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)
        df_all=FEsingle.PredictDaysTrend(df_all,5)

        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>2]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
        #df_all=df_all[df_all['total_mv_rank']>15]
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
        df_money_all=pd.read_csv('real_moneyflow_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos'].shift(1)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos'].shift(1)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos'].shift(1)

        df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)


        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='left', on=['ts_code','trade_date'])
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

        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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


        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'max')

        df_all,_=FEsingle.HighLowRange(df_all,8)
        df_all,_=FEsingle.HighLowRange(df_all,25)  
        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2
        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)

        df_all=FEsingle.AmountChgRank(df_all,12)

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12','real_price_pos'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['chg_rank']>0.7]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)

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

class FE_a29_big(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_money_all=pd.read_csv(DataSetName[3],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos']
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos']
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos']

        #df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        #df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        #df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)

        df_money_all=FEsingle.InputChgSum(df_money_all,5,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,12,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,25,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'net_mf_amount')

        #df_money_all['sm_amount_25_diff']=df_money_all['sm_amount_25']-df_money_all['sm_amount_12']
        #df_money_all['sm_amount_12_diff']=df_money_all['sm_amount_12']-df_money_all['sm_amount_5']

        #df_money_all['lg_amount_25_diff']=df_money_all['lg_amount_25']-df_money_all['lg_amount_12']
        #df_money_all['lg_amount_12_diff']=df_money_all['lg_amount_12']-df_money_all['lg_amount_5']

        #df_money_all['net_mf_amount_25_diff']=df_money_all['net_mf_amount_25']-df_money_all['net_mf_amount_12']
        #df_money_all['net_mf_amount_12_diff']=df_money_all['net_mf_amount_12']-df_money_all['net_mf_amount_5']


        print(df_money_all)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        df_all['dayofweek']=pd.to_datetime(df_all['trade_date'],format='%Y%m%d')
        df_all['dayofweek']=df_all['dayofweek'].dt.dayofweek

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
        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'max')

        df_all,_=FEsingle.HighLowRange(df_all,5)
        df_all,_=FEsingle.HighLowRange(df_all,12)
        df_all,_=FEsingle.HighLowRange(df_all,25)    

        df_all['25_pct_rank_min_diff']=df_all['25_pct_rank_min']-df_all['12_pct_rank_min']
        df_all['12_pct_rank_min_diff']=df_all['12_pct_rank_min']-df_all['5_pct_rank_min']

        df_all['25_pct_rank_max_diff']=df_all['25_pct_rank_max']-df_all['12_pct_rank_max']
        df_all['12_pct_rank_max_diff']=df_all['12_pct_rank_max']-df_all['5_pct_rank_max']

        df_all['25_pct_Rangerank_diff']=df_all['25_pct_Rangerank']-df_all['12_pct_Rangerank']
        df_all['12_pct_Rangerank_diff']=df_all['12_pct_Rangerank']-df_all['5_pct_Rangerank']

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

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

        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        
        df_all=FEsingle.PctChgAbsSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)
        df_all=FEsingle.PctChgSumRank(df_all,24)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)
        df_all=FEsingle.PctChgSum(df_all,24)

        df_all['chg_rank_24_diff']=df_all['chg_rank_24']-df_all['chg_rank_12']
        df_all['chg_rank_12_diff']=df_all['chg_rank_12']-df_all['chg_rank_6']
        df_all['chg_rank_6_diff']=df_all['chg_rank_6']-df_all['chg_rank_3']

        df_all['pct_chg_24_diff']=df_all['pct_chg_24']-df_all['pct_chg_12']
        df_all['pct_chg_12_diff']=df_all['pct_chg_12']-df_all['pct_chg_6']
        df_all['pct_chg_6_diff']=df_all['pct_chg_6']-df_all['pct_chg_3']


        #df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','real_price_pos'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)
        df_all=FEsingle.PredictDaysTrend(df_all,5)

        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>2]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
        df_all=df_all[df_all['total_mv_rank']>15]
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
        df_money_all=pd.read_csv('real_moneyflow_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos'].shift(1)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos'].shift(1)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos'].shift(1)

        df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)


        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='left', on=['ts_code','trade_date'])
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

        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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


        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'max')

        df_all,_=FEsingle.HighLowRange(df_all,8)
        df_all,_=FEsingle.HighLowRange(df_all,25)  
        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2
        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)

        df_all=FEsingle.AmountChgRank(df_all,12)

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12','real_price_pos'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['chg_rank']>0.7]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)

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

class FE_a29_small(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_money_all=pd.read_csv(DataSetName[3],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos']
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos']
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos']

        #df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        #df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        #df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)

        df_money_all=FEsingle.InputChgSum(df_money_all,5,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,12,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,25,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'net_mf_amount')

        #df_money_all['sm_amount_25_diff']=df_money_all['sm_amount_25']-df_money_all['sm_amount_12']
        #df_money_all['sm_amount_12_diff']=df_money_all['sm_amount_12']-df_money_all['sm_amount_5']

        #df_money_all['lg_amount_25_diff']=df_money_all['lg_amount_25']-df_money_all['lg_amount_12']
        #df_money_all['lg_amount_12_diff']=df_money_all['lg_amount_12']-df_money_all['lg_amount_5']

        #df_money_all['net_mf_amount_25_diff']=df_money_all['net_mf_amount_25']-df_money_all['net_mf_amount_12']
        #df_money_all['net_mf_amount_12_diff']=df_money_all['net_mf_amount_12']-df_money_all['net_mf_amount_5']


        print(df_money_all)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        df_all['dayofweek']=pd.to_datetime(df_all['trade_date'],format='%Y%m%d')
        df_all['dayofweek']=df_all['dayofweek'].dt.dayofweek

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
        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'max')

        df_all,_=FEsingle.HighLowRange(df_all,5)
        df_all,_=FEsingle.HighLowRange(df_all,12)
        df_all,_=FEsingle.HighLowRange(df_all,25)    

        df_all['25_pct_rank_min_diff']=df_all['25_pct_rank_min']-df_all['12_pct_rank_min']
        df_all['12_pct_rank_min_diff']=df_all['12_pct_rank_min']-df_all['5_pct_rank_min']

        df_all['25_pct_rank_max_diff']=df_all['25_pct_rank_max']-df_all['12_pct_rank_max']
        df_all['12_pct_rank_max_diff']=df_all['12_pct_rank_max']-df_all['5_pct_rank_max']

        df_all['25_pct_Rangerank_diff']=df_all['25_pct_Rangerank']-df_all['12_pct_Rangerank']
        df_all['12_pct_Rangerank_diff']=df_all['12_pct_Rangerank']-df_all['5_pct_Rangerank']

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

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

        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        
        df_all=FEsingle.PctChgAbsSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)
        df_all=FEsingle.PctChgSumRank(df_all,24)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)
        df_all=FEsingle.PctChgSum(df_all,24)

        df_all['chg_rank_24_diff']=df_all['chg_rank_24']-df_all['chg_rank_12']
        df_all['chg_rank_12_diff']=df_all['chg_rank_12']-df_all['chg_rank_6']
        df_all['chg_rank_6_diff']=df_all['chg_rank_6']-df_all['chg_rank_3']

        df_all['pct_chg_24_diff']=df_all['pct_chg_24']-df_all['pct_chg_12']
        df_all['pct_chg_12_diff']=df_all['pct_chg_12']-df_all['pct_chg_6']
        df_all['pct_chg_6_diff']=df_all['pct_chg_6']-df_all['pct_chg_3']


        #df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','real_price_pos'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)
        df_all=FEsingle.PredictDaysTrend(df_all,5)

        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>2]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
        df_all=df_all[df_all['total_mv_rank']<10]
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
        df_money_all=pd.read_csv('real_moneyflow_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos'].shift(1)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos'].shift(1)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos'].shift(1)

        df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)


        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='left', on=['ts_code','trade_date'])
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

        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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


        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'max')

        df_all,_=FEsingle.HighLowRange(df_all,8)
        df_all,_=FEsingle.HighLowRange(df_all,25)  
        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2
        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)

        df_all=FEsingle.AmountChgRank(df_all,12)

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12','real_price_pos'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['chg_rank']>0.7]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)

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

class FE_a29_Volatility(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_money_all=pd.read_csv(DataSetName[3],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos']
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos']
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos']

        #df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        #df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        #df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)

        df_money_all=FEsingle.InputChgSum(df_money_all,5,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,12,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,25,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'net_mf_amount')

        #df_money_all['sm_amount_25_diff']=df_money_all['sm_amount_25']-df_money_all['sm_amount_12']
        #df_money_all['sm_amount_12_diff']=df_money_all['sm_amount_12']-df_money_all['sm_amount_5']

        #df_money_all['lg_amount_25_diff']=df_money_all['lg_amount_25']-df_money_all['lg_amount_12']
        #df_money_all['lg_amount_12_diff']=df_money_all['lg_amount_12']-df_money_all['lg_amount_5']

        #df_money_all['net_mf_amount_25_diff']=df_money_all['net_mf_amount_25']-df_money_all['net_mf_amount_12']
        #df_money_all['net_mf_amount_12_diff']=df_money_all['net_mf_amount_12']-df_money_all['net_mf_amount_5']


        print(df_money_all)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        df_all['dayofweek']=pd.to_datetime(df_all['trade_date'],format='%Y%m%d')
        df_all['dayofweek']=df_all['dayofweek'].dt.dayofweek

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
        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'max')

        df_all,_=FEsingle.HighLowRange(df_all,5)
        df_all,_=FEsingle.HighLowRange(df_all,12)
        df_all,_=FEsingle.HighLowRange(df_all,25)    

        df_all['25_pct_rank_min_diff']=df_all['25_pct_rank_min']-df_all['12_pct_rank_min']
        df_all['12_pct_rank_min_diff']=df_all['12_pct_rank_min']-df_all['5_pct_rank_min']

        df_all['25_pct_rank_max_diff']=df_all['25_pct_rank_max']-df_all['12_pct_rank_max']
        df_all['12_pct_rank_max_diff']=df_all['12_pct_rank_max']-df_all['5_pct_rank_max']

        df_all['25_pct_Rangerank_diff']=df_all['25_pct_Rangerank']-df_all['12_pct_Rangerank']
        df_all['12_pct_Rangerank_diff']=df_all['12_pct_Rangerank']-df_all['5_pct_Rangerank']

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

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

        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        
        df_all=FEsingle.PctChgAbsSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)
        df_all=FEsingle.PctChgSumRank(df_all,24)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)
        df_all=FEsingle.PctChgSum(df_all,24)

        df_all['chg_rank_24_diff']=df_all['chg_rank_24']-df_all['chg_rank_12']
        df_all['chg_rank_12_diff']=df_all['chg_rank_12']-df_all['chg_rank_6']
        df_all['chg_rank_6_diff']=df_all['chg_rank_6']-df_all['chg_rank_3']

        df_all['pct_chg_24_diff']=df_all['pct_chg_24']-df_all['pct_chg_12']
        df_all['pct_chg_12_diff']=df_all['pct_chg_12']-df_all['pct_chg_6']
        df_all['pct_chg_6_diff']=df_all['pct_chg_6']-df_all['pct_chg_3']


        #df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','real_price_pos'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDaysTrend(df_all,5)


        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>2]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
        #df_all=df_all[df_all['total_mv_rank']>15]
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
        df_money_all=pd.read_csv('real_moneyflow_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos'].shift(1)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos'].shift(1)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos'].shift(1)

        df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)


        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='left', on=['ts_code','trade_date'])
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

        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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


        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'max')

        df_all,_=FEsingle.HighLowRange(df_all,8)
        df_all,_=FEsingle.HighLowRange(df_all,25)  
        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2
        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)

        df_all=FEsingle.AmountChgRank(df_all,12)

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12','real_price_pos'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['chg_rank']>0.7]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)

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

class FE_a31(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_money_all=pd.read_csv(DataSetName[3],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos']
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos']
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos']

        #df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        #df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        #df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)

        df_money_all=FEsingle.InputChgSum(df_money_all,5,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,12,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,25,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'net_mf_amount')

        #df_money_all['sm_amount_25_diff']=df_money_all['sm_amount_25']-df_money_all['sm_amount_12']
        #df_money_all['sm_amount_12_diff']=df_money_all['sm_amount_12']-df_money_all['sm_amount_5']

        #df_money_all['lg_amount_25_diff']=df_money_all['lg_amount_25']-df_money_all['lg_amount_12']
        #df_money_all['lg_amount_12_diff']=df_money_all['lg_amount_12']-df_money_all['lg_amount_5']

        #df_money_all['net_mf_amount_25_diff']=df_money_all['net_mf_amount_25']-df_money_all['net_mf_amount_12']
        #df_money_all['net_mf_amount_12_diff']=df_money_all['net_mf_amount_12']-df_money_all['net_mf_amount_5']


        print(df_money_all)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        df_all['dayofweek']=pd.to_datetime(df_all['trade_date'],format='%Y%m%d')
        df_all['dayofweek']=df_all['dayofweek'].dt.dayofweek

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
        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'max')

        df_all,_=FEsingle.HighLowRange(df_all,5)
        df_all,_=FEsingle.HighLowRange(df_all,12)
        df_all,_=FEsingle.HighLowRange(df_all,25)    

        df_all['25_pct_rank_min_diff']=df_all['25_pct_rank_min']-df_all['12_pct_rank_min']
        df_all['12_pct_rank_min_diff']=df_all['12_pct_rank_min']-df_all['5_pct_rank_min']

        df_all['25_pct_rank_max_diff']=df_all['25_pct_rank_max']-df_all['12_pct_rank_max']
        df_all['12_pct_rank_max_diff']=df_all['12_pct_rank_max']-df_all['5_pct_rank_max']

        df_all['25_pct_Rangerank_diff']=df_all['25_pct_Rangerank']-df_all['12_pct_Rangerank']
        df_all['12_pct_Rangerank_diff']=df_all['12_pct_Rangerank']-df_all['5_pct_Rangerank']

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

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

        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        
        df_all=FEsingle.PctChgAbsSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)
        df_all=FEsingle.PctChgSumRank(df_all,24)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)
        df_all=FEsingle.PctChgSum(df_all,24)

        df_all['chg_rank_24_diff']=df_all['chg_rank_24']-df_all['chg_rank_12']
        df_all['chg_rank_12_diff']=df_all['chg_rank_12']-df_all['chg_rank_6']
        df_all['chg_rank_6_diff']=df_all['chg_rank_6']-df_all['chg_rank_3']

        df_all['pct_chg_24_diff']=df_all['pct_chg_24']-df_all['pct_chg_12']
        df_all['pct_chg_12_diff']=df_all['pct_chg_12']-df_all['pct_chg_6']
        df_all['pct_chg_6_diff']=df_all['pct_chg_6']-df_all['pct_chg_3']


        #df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','real_price_pos'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDaysTrend(df_all,5)

        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>2]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
        df_all=df_all[df_all['total_mv_rank']<6]
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
        df_money_all=pd.read_csv('real_moneyflow_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos'].shift(1)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos'].shift(1)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos'].shift(1)

        df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)


        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='left', on=['ts_code','trade_date'])
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

        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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


        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'max')

        df_all,_=FEsingle.HighLowRange(df_all,8)
        df_all,_=FEsingle.HighLowRange(df_all,25)  
        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2
        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)

        df_all=FEsingle.AmountChgRank(df_all,12)

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12','real_price_pos'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['chg_rank']>0.7]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)

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

class FE_a31_full(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_money_all=pd.read_csv(DataSetName[3],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos']
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos']
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos']

        #df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        #df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        #df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)

        df_money_all=FEsingle.InputChgSum(df_money_all,5,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,12,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,25,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'net_mf_amount')

        #df_money_all['sm_amount_25_diff']=df_money_all['sm_amount_25']-df_money_all['sm_amount_12']
        #df_money_all['sm_amount_12_diff']=df_money_all['sm_amount_12']-df_money_all['sm_amount_5']

        #df_money_all['lg_amount_25_diff']=df_money_all['lg_amount_25']-df_money_all['lg_amount_12']
        #df_money_all['lg_amount_12_diff']=df_money_all['lg_amount_12']-df_money_all['lg_amount_5']

        #df_money_all['net_mf_amount_25_diff']=df_money_all['net_mf_amount_25']-df_money_all['net_mf_amount_12']
        #df_money_all['net_mf_amount_12_diff']=df_money_all['net_mf_amount_12']-df_money_all['net_mf_amount_5']


        print(df_money_all)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        #df_all['st_or_otherwrong']=0
        #df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        df_all['dayofweek']=pd.to_datetime(df_all['trade_date'],format='%Y%m%d')
        df_all['dayofweek']=df_all['dayofweek'].dt.dayofweek

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
        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'max')

        df_all,_=FEsingle.HighLowRange(df_all,5)
        df_all,_=FEsingle.HighLowRange(df_all,12)
        df_all,_=FEsingle.HighLowRange(df_all,25)    

        df_all['25_pct_rank_min_diff']=df_all['25_pct_rank_min']-df_all['12_pct_rank_min']
        df_all['12_pct_rank_min_diff']=df_all['12_pct_rank_min']-df_all['5_pct_rank_min']

        df_all['25_pct_rank_max_diff']=df_all['25_pct_rank_max']-df_all['12_pct_rank_max']
        df_all['12_pct_rank_max_diff']=df_all['12_pct_rank_max']-df_all['5_pct_rank_max']

        df_all['25_pct_Rangerank_diff']=df_all['25_pct_Rangerank']-df_all['12_pct_Rangerank']
        df_all['12_pct_Rangerank_diff']=df_all['12_pct_Rangerank']-df_all['5_pct_Rangerank']

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

        #是否停
        #df_all['high_stop']=0
        #df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2

        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        
        df_all=FEsingle.PctChgAbsSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)
        df_all=FEsingle.PctChgSumRank(df_all,24)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)
        df_all=FEsingle.PctChgSum(df_all,24)

        df_all['chg_rank_24_diff']=df_all['chg_rank_24']-df_all['chg_rank_12']
        df_all['chg_rank_12_diff']=df_all['chg_rank_12']-df_all['chg_rank_6']
        df_all['chg_rank_6_diff']=df_all['chg_rank_6']-df_all['chg_rank_3']

        df_all['pct_chg_24_diff']=df_all['pct_chg_24']-df_all['pct_chg_12']
        df_all['pct_chg_12_diff']=df_all['pct_chg_12']-df_all['pct_chg_6']
        df_all['pct_chg_6_diff']=df_all['pct_chg_6']-df_all['pct_chg_3']


        #df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','real_price_pos'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDaysTrend(df_all,5)

        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>2]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
        df_all=df_all[df_all['total_mv_rank']<6]
        #df_all=df_all[df_all['total_mv_rank']>2]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['circ_mv_pct']>3]
        #df_all=df_all[df_all['ps_ttm']>3]
        #df_all=df_all[df_all['pb_rank']>3]

        #暂时不用的列
        #df_all=df_all[df_all['high_stop']==0]
        #df_all=df_all[df_all['st_or_otherwrong']==1]
        #'tomorrow_chg'
        df_all.drop(['amount','close','real_price'],axis=1,inplace=True)
        #df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)
        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):
        #新模型预定版本

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)
        df_money_all=pd.read_csv('real_moneyflow_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos'].shift(1)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos'].shift(1)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos'].shift(1)

        df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)


        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='left', on=['ts_code','trade_date'])
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

        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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


        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'max')

        df_all,_=FEsingle.HighLowRange(df_all,8)
        df_all,_=FEsingle.HighLowRange(df_all,25)  
        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2
        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)

        df_all=FEsingle.AmountChgRank(df_all,12)

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12','real_price_pos'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['chg_rank']>0.7]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)

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

class FE_a29_full(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_money_all=pd.read_csv(DataSetName[3],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos']
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos']
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos']

        #df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        #df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        #df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)

        df_money_all=FEsingle.InputChgSum(df_money_all,5,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,12,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,25,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'net_mf_amount')

        #df_money_all['sm_amount_25_diff']=df_money_all['sm_amount_25']-df_money_all['sm_amount_12']
        #df_money_all['sm_amount_12_diff']=df_money_all['sm_amount_12']-df_money_all['sm_amount_5']

        #df_money_all['lg_amount_25_diff']=df_money_all['lg_amount_25']-df_money_all['lg_amount_12']
        #df_money_all['lg_amount_12_diff']=df_money_all['lg_amount_12']-df_money_all['lg_amount_5']

        #df_money_all['net_mf_amount_25_diff']=df_money_all['net_mf_amount_25']-df_money_all['net_mf_amount_12']
        #df_money_all['net_mf_amount_12_diff']=df_money_all['net_mf_amount_12']-df_money_all['net_mf_amount_5']


        print(df_money_all)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        #df_all['st_or_otherwrong']=0
        #df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        df_all['dayofweek']=pd.to_datetime(df_all['trade_date'],format='%Y%m%d')
        df_all['dayofweek']=df_all['dayofweek'].dt.dayofweek

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
        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'max')

        df_all,_=FEsingle.HighLowRange(df_all,5)
        df_all,_=FEsingle.HighLowRange(df_all,12)
        df_all,_=FEsingle.HighLowRange(df_all,25)    

        df_all['25_pct_rank_min_diff']=df_all['25_pct_rank_min']-df_all['12_pct_rank_min']
        df_all['12_pct_rank_min_diff']=df_all['12_pct_rank_min']-df_all['5_pct_rank_min']

        df_all['25_pct_rank_max_diff']=df_all['25_pct_rank_max']-df_all['12_pct_rank_max']
        df_all['12_pct_rank_max_diff']=df_all['12_pct_rank_max']-df_all['5_pct_rank_max']

        df_all['25_pct_Rangerank_diff']=df_all['25_pct_Rangerank']-df_all['12_pct_Rangerank']
        df_all['12_pct_Rangerank_diff']=df_all['12_pct_Rangerank']-df_all['5_pct_Rangerank']

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

        #是否停
        #df_all['high_stop']=0
        #df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2

        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        
        df_all=FEsingle.PctChgAbsSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)
        df_all=FEsingle.PctChgSumRank(df_all,24)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)
        df_all=FEsingle.PctChgSum(df_all,24)

        df_all['chg_rank_24_diff']=df_all['chg_rank_24']-df_all['chg_rank_12']
        df_all['chg_rank_12_diff']=df_all['chg_rank_12']-df_all['chg_rank_6']
        df_all['chg_rank_6_diff']=df_all['chg_rank_6']-df_all['chg_rank_3']

        df_all['pct_chg_24_diff']=df_all['pct_chg_24']-df_all['pct_chg_12']
        df_all['pct_chg_12_diff']=df_all['pct_chg_12']-df_all['pct_chg_6']
        df_all['pct_chg_6_diff']=df_all['pct_chg_6']-df_all['pct_chg_3']


        #df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','real_price_pos'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDaysTrend(df_all,5)

        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>2]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
        #df_all=df_all[df_all['total_mv_rank']<6]
        #df_all=df_all[df_all['total_mv_rank']>2]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['circ_mv_pct']>3]
        #df_all=df_all[df_all['ps_ttm']>3]
        #df_all=df_all[df_all['pb_rank']>3]

        #暂时不用的列
        #df_all=df_all[df_all['high_stop']==0]
        #df_all=df_all[df_all['st_or_otherwrong']==1]
        #'tomorrow_chg'
        df_all.drop(['amount','close','real_price'],axis=1,inplace=True)
        #df_all.drop(['st_or_otherwrong'],axis=1,inplace=True)
        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):
        #新模型预定版本

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)
        df_money_all=pd.read_csv('real_moneyflow_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos'].shift(1)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos'].shift(1)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos'].shift(1)

        df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)


        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='left', on=['ts_code','trade_date'])
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

        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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


        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'max')

        df_all,_=FEsingle.HighLowRange(df_all,8)
        df_all,_=FEsingle.HighLowRange(df_all,25)  
        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2
        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)

        df_all=FEsingle.AmountChgRank(df_all,12)

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12','real_price_pos'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['chg_rank']>0.7]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)

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

class FE_qliba2(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        #df_all['real_open']=df_all['adj_factor']*df_all['open']
        df_all=FEsingle.PredictDaysTrend(df_all,5)

        print(df_all)
        df_all=df_all.loc[:,['ts_code','trade_date','tomorrow_chg','tomorrow_chg_rank']]

        print(df_all.dtypes)
        print(df_all)
        #===================================================================================================================================#
        #获取qlib特征

        ###df_qlib_1=pd.read_csv('zzztest.csv',header=0)
        ###df_qlib_2=pd.read_csv('zzztest2.csv',header=0)
        ##df_qlib_1=pd.read_csv('2013.csv',header=0)
        ###df_qlib_1=df_qlib_1.iloc[:,0:70]

        ##df_qlib_all_l=df_qlib_1.iloc[:,0:2]
        ##df_qlib_all_r=df_qlib_1.iloc[:,70:]
        ##df_qlib_1 = pd.concat([df_qlib_all_l,df_qlib_all_r],axis=1)
        ##print(df_qlib_1.head(10))

        ##df_qlib_2=pd.read_csv('2015.csv',header=0)

        ##df_qlib_all_l=df_qlib_2.iloc[:,0:2]
        ##df_qlib_all_r=df_qlib_2.iloc[:,70:]
        ##df_qlib_2 = pd.concat([df_qlib_all_l,df_qlib_all_r],axis=1)

        ##df_qlib_3=pd.read_csv('2017.csv',header=0)
        ##df_qlib_all_l=df_qlib_3.iloc[:,0:2]
        ##df_qlib_all_r=df_qlib_3.iloc[:,70:]
        ##df_qlib_3 = pd.concat([df_qlib_all_l,df_qlib_all_r],axis=1)
        ##df_qlib_4=pd.read_csv('2019.csv',header=0)
        ##df_qlib_all_l=df_qlib_4.iloc[:,0:2]
        ##df_qlib_all_r=df_qlib_4.iloc[:,70:]
        ##df_qlib_4 = pd.concat([df_qlib_all_l,df_qlib_all_r],axis=1)

        ##df_qlib_all=pd.concat([df_qlib_2,df_qlib_1])
        ##df_qlib_all=pd.concat([df_qlib_3,df_qlib_all])
        ##df_qlib_all=pd.concat([df_qlib_4,df_qlib_all])

        ##df_qlib_all.drop_duplicates()

        ##print(df_qlib_all.head(10))

        ##df_qlib_all.drop(['LABEL0'],axis=1,inplace=True)

        ##df_qlib_all.to_csv("13to21_first70plus.csv")


        df_qlib_all=pd.read_csv('13to21_first70plus.csv',header=0)
        #df_qlib_all.drop(['LABEL0'],axis=1,inplace=True)

        print(df_qlib_all)

        df_qlib_all.rename(columns={'datetime':'trade_date','instrument':'ts_code','score':'mix'}, inplace = True)
        print(df_qlib_all.dtypes)
        print(df_qlib_all)
        df_qlib_all['trade_date'] = pd.to_datetime(df_qlib_all['trade_date'], format='%Y-%m-%d')
        df_qlib_all['trade_date']=df_qlib_all['trade_date'].apply(lambda x: x.strftime('%Y%m%d'))
        df_qlib_all['trade_date'] = df_qlib_all['trade_date'].astype(int)

        df_qlib_all['ts_codeL'] = df_qlib_all['ts_code'].str[:2]
        df_qlib_all['ts_codeR'] = df_qlib_all['ts_code'].str[2:]

        df_qlib_all['ts_codeR'] = df_qlib_all['ts_codeR'].apply(lambda s: s+'.')

        df_qlib_all['ts_code']=df_qlib_all['ts_codeR'].str.cat(df_qlib_all['ts_codeL'])

        df_qlib_all.drop(['ts_codeL','ts_codeR'],axis=1,inplace=True)
        print(df_qlib_all.dtypes)
        print(df_qlib_all)

        df_qlib_all=df_qlib_all.fillna(value=0)

        df_all=pd.merge(df_all, df_qlib_all, how='left', on=['ts_code','trade_date'])

        print(df_all)

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):
        #新模型预定版本

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)
        df_money_all=pd.read_csv('real_moneyflow_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos'].shift(1)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos'].shift(1)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos'].shift(1)

        df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)


        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='left', on=['ts_code','trade_date'])
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

        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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


        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'max')

        df_all,_=FEsingle.HighLowRange(df_all,8)
        df_all,_=FEsingle.HighLowRange(df_all,25)  
        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2
        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)

        df_all=FEsingle.AmountChgRank(df_all,12)

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12','real_price_pos'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['chg_rank']>0.7]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)

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

class FEonlinew_a31(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_money_all=pd.read_csv(DataSetName[3],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos']
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos']
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos']

        df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)

        df_money_all=FEsingle.InputChgSum(df_money_all,5,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,12,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,12,'net_mf_amount')

        df_money_all=FEsingle.InputChgSum(df_money_all,25,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,25,'net_mf_amount')

        print(df_money_all)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='inner', on=['ts_code','trade_date'])
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
        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'max')

        df_all,_=FEsingle.HighLowRange(df_all,5)
        df_all,_=FEsingle.HighLowRange(df_all,12)
        df_all,_=FEsingle.HighLowRange(df_all,25)    

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

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

        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        
        df_all=FEsingle.PctChgAbsSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)
        df_all=FEsingle.PctChgSumRank(df_all,24)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)
        df_all=FEsingle.PctChgSum(df_all,24)


        #df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','real_price_pos'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDaysTrend(df_all,5)


        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>2]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
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
        df_money_all=pd.read_csv('real_moneyflow_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='left', on=['ts_code','trade_date'])

        df_all['sm_amount']=df_all.groupby('ts_code')['sm_amount'].shift(1)
        df_all['lg_amount']=df_all.groupby('ts_code')['lg_amount'].shift(1)
        df_all['net_mf_amount']=df_all.groupby('ts_code')['net_mf_amount'].shift(1)

        df_all=FEsingle.InputChgSum(df_all,5,'sm_amount')
        df_all=FEsingle.InputChgSum(df_all,5,'lg_amount')
        df_all=FEsingle.InputChgSum(df_all,5,'net_mf_amount')

        df_all=FEsingle.InputChgSum(df_all,12,'sm_amount')
        df_all=FEsingle.InputChgSum(df_all,12,'lg_amount')
        df_all=FEsingle.InputChgSum(df_all,12,'net_mf_amount')

        df_all=FEsingle.InputChgSum(df_all,25,'sm_amount')
        df_all=FEsingle.InputChgSum(df_all,25,'lg_amount')
        df_all=FEsingle.InputChgSum(df_all,25,'net_mf_amount')

        df_all=pd.merge(df_all, df_long_all, how='left', on=['ts_code','trade_date'])

        print(df_all)

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)


        df_all['dayofweek']=pd.to_datetime(df_all['trade_date'],format='%Y%m%d')
        df_all['dayofweek']=df_all['dayofweek'].dt.dayofweek


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

        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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


        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'max')

        df_all,_=FEsingle.HighLowRange(df_all,5)
        df_all,_=FEsingle.HighLowRange(df_all,12)
        df_all,_=FEsingle.HighLowRange(df_all,25) 
        #===================================================================================================================================#

        df_all['25_pct_rank_min_diff']=df_all['25_pct_rank_min']-df_all['12_pct_rank_min']
        df_all['12_pct_rank_min_diff']=df_all['12_pct_rank_min']-df_all['5_pct_rank_min']

        df_all['25_pct_rank_max_diff']=df_all['25_pct_rank_max']-df_all['12_pct_rank_max']
        df_all['12_pct_rank_max_diff']=df_all['12_pct_rank_max']-df_all['5_pct_rank_max']

        df_all['25_pct_Rangerank_diff']=df_all['25_pct_Rangerank']-df_all['12_pct_Rangerank']
        df_all['12_pct_Rangerank_diff']=df_all['12_pct_Rangerank']-df_all['5_pct_Rangerank']



        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2
        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        df_all=FEsingle.PctChgAbsSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)
        df_all=FEsingle.PctChgSumRank(df_all,24)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)
        df_all=FEsingle.PctChgSum(df_all,24)

        df_all['chg_rank_24_diff']=df_all['chg_rank_24']-df_all['chg_rank_12']
        df_all['chg_rank_12_diff']=df_all['chg_rank_12']-df_all['chg_rank_6']
        df_all['chg_rank_6_diff']=df_all['chg_rank_6']-df_all['chg_rank_3']

        df_all['pct_chg_24_diff']=df_all['pct_chg_24']-df_all['pct_chg_12']
        df_all['pct_chg_12_diff']=df_all['pct_chg_12']-df_all['pct_chg_6']
        df_all['pct_chg_6_diff']=df_all['pct_chg_6']-df_all['pct_chg_3']

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','real_price_pos'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>2]
        #df_all=df_all[df_all['chg_rank']>0.7]
        df_all=df_all[df_all['amount']>15000]
        df_all=df_all[df_all['total_mv_rank']<6]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)

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

class FEfast_a23(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_money_all=pd.read_csv(DataSetName[3],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos'].shift(1)
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos']
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos']

        df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)

        df_money_all=FEsingle.InputChgSum(df_money_all,5,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'net_mf_amount')

        #print(df_money_all)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        df_all['dayofweek']=pd.to_datetime(df_all['trade_date'],format='%Y%m%d')
        df_all['dayofweek']=df_all['dayofweek'].dt.dayofweek

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #df_all['ts_code_try']=df_all['ts_code'].map(lambda x : x[:-3])
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        #df_all['real_open']=df_all['adj_factor']*df_all['open']
        #===================================================================================================================================#
        #df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)


        #df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        #df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        #df_all['total_mv_rank']=df_all['total_mv_rank']*19.9//1


        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'max')

        df_all,_=FEsingle.HighLowRange(df_all,5)
        df_all,_=FEsingle.HighLowRange(df_all,12)
        df_all,_=FEsingle.HighLowRange(df_all,25)   

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)


        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        
        #df_all=FEsingle.PctChgAbsSumRank(df_all,6)
        #df_all=FEsingle.PctChgSumRank(df_all,3)
        #df_all=FEsingle.PctChgSumRank(df_all,6)
        #df_all=FEsingle.PctChgSumRank(df_all,12)
        #df_all=FEsingle.PctChgSumRank(df_all,24)

        df_all=FEsingle.PctChgSum(df_all,3)
        #df_all=FEsingle.PctChgSum(df_all,6)
        #df_all=FEsingle.PctChgSum(df_all,12)
        #df_all=FEsingle.PctChgSum(df_all,24)

        #df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,24)   

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*19.9//2


        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12','real_price_pos'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','sm_amount','lg_amount','net_mf_amount'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDaysTrend(df_all,5)


        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>2]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
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
        df_money_all=pd.read_csv('real_moneyflow_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos'].shift(1)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos'].shift(1)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos'].shift(1)

        df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)


        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='left', on=['ts_code','trade_date'])
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

        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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


        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'max')

        df_all,_=FEsingle.HighLowRange(df_all,8)
        df_all,_=FEsingle.HighLowRange(df_all,25)  
        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2
        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)

        df_all=FEsingle.AmountChgRank(df_all,12)

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12','real_price_pos'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['chg_rank']>0.7]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)

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

class FEfast_a23_pos(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_money_all=pd.read_csv(DataSetName[3],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        #df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos'].shift(1)
        #df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos']
        #df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos']

        df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)

        df_money_all=FEsingle.InputChgSum(df_money_all,5,'sm_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'lg_amount')
        df_money_all=FEsingle.InputChgSum(df_money_all,5,'net_mf_amount')

        #print(df_money_all)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        df_all['dayofweek']=pd.to_datetime(df_all['trade_date'],format='%Y%m%d')
        df_all['dayofweek']=df_all['dayofweek'].dt.dayofweek

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        df_all['class1']=0
        df_all.loc[df_all['ts_code'].str.startswith('30')==True,'class1']=1
        df_all.loc[df_all['ts_code'].str.startswith('60')==True,'class1']=2
        df_all.loc[df_all['ts_code'].str.startswith('00')==True,'class1']=3
        
        #df_all['ts_code_try']=df_all['ts_code'].map(lambda x : x[:-3])
        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        #df_all['real_open']=df_all['adj_factor']*df_all['open']
        #===================================================================================================================================#
        df_all['tomorrow_chg_rank']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPy(x)).reset_index(0,drop=True)
        df_all['tomorrow_chg_rank']=df_all.groupby('ts_code')['tomorrow_chg_rank'].shift(-20)
        df_all['tomorrow_chg']=df_all['tomorrow_chg_rank']
        

        #df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        #df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        #df_all['total_mv_rank']=df_all['total_mv_rank']*19.9//1


        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('ts_code')['pb_rank'].shift(1)

        df_all['circ_mv_pct']=(df_all['total_mv']-df_all['circ_mv'])/df_all['total_mv']
        df_all['circ_mv_pct']=df_all.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df_all['circ_mv_pct']=df_all.groupby('ts_code')['circ_mv_pct'].shift(1)
        
        df_all['ps_ttm']=df_all.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df_all['ps_ttm']=df_all.groupby('ts_code')['ps_ttm'].shift(1)
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,5,'max')

        df_all,_=FEsingle.HighLowRange(df_all,5)
        df_all,_=FEsingle.HighLowRange(df_all,12)
        df_all,_=FEsingle.HighLowRange(df_all,25)   

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)


        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        
        #df_all=FEsingle.PctChgAbsSumRank(df_all,6)
        #df_all=FEsingle.PctChgSumRank(df_all,3)
        #df_all=FEsingle.PctChgSumRank(df_all,6)
        #df_all=FEsingle.PctChgSumRank(df_all,12)
        #df_all=FEsingle.PctChgSumRank(df_all,24)

        df_all=FEsingle.PctChgSum(df_all,3)
        #df_all=FEsingle.PctChgSum(df_all,6)
        #df_all=FEsingle.PctChgSum(df_all,12)
        #df_all=FEsingle.PctChgSum(df_all,24)

        #df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,24)   

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*19.9//2


        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12','real_price_pos'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','sm_amount','lg_amount','net_mf_amount'],1)
        df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        #df_all=FEsingle.PredictDaysTrend(df_all,5)


        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>2]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
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
        df_money_all=pd.read_csv('real_moneyflow_now.csv',index_col=0,header=0)
        df_long_all=pd.read_csv('real_long_now.csv',index_col=0,header=0)

        df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        df_money_all['sm_amount_pos']=df_money_all.groupby('ts_code')['sm_amount_pos'].shift(1)
        df_money_all['lg_amount_pos']=df_money_all.groupby('ts_code')['lg_amount_pos'].shift(1)
        df_money_all['net_mf_amount_pos']=df_money_all.groupby('ts_code')['net_mf_amount_pos'].shift(1)

        df_money_all['sm_amount']=df_money_all.groupby('ts_code')['sm_amount'].shift(1)
        df_money_all['lg_amount']=df_money_all.groupby('ts_code')['lg_amount'].shift(1)
        df_money_all['net_mf_amount']=df_money_all.groupby('ts_code')['net_mf_amount'].shift(1)


        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_money_all, how='left', on=['ts_code','trade_date'])
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

        df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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


        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow(df_all,8,'max')

        df_all,_=FEsingle.HighLowRange(df_all,8)
        df_all,_=FEsingle.HighLowRange(df_all,25)  
        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2
        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        df_all=FEsingle.PctChgSumRank(df_all,3)
        df_all=FEsingle.PctChgSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,12)

        df_all=FEsingle.PctChgSum(df_all,3)
        df_all=FEsingle.PctChgSum(df_all,6)
        df_all=FEsingle.PctChgSum(df_all,12)

        df_all=FEsingle.AmountChgRank(df_all,12)

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*10//2

        #df_all=FEsingle.PctChgSumRank_Common(df_all,5,'high')
            

        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12','real_price_pos'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],2)
        #df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pst_amount_rank_12'],3)


        #删除市值过低的票
        df_all=df_all[df_all['close']>3]
        #df_all=df_all[df_all['chg_rank']>0.7]
        df_all=df_all[df_all['amount']>15000]
        #df_all=df_all[df_all['total_mv_rank']<12]

        df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price','amount','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)

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

class FEfast_a41(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        #df_money_all=pd.read_csv(DataSetName[3],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        #df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        #df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        #df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        #df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        #df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        #df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        #df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)


        #df_money_all=FEsingle.InputChgSum(df_money_all,5,'sm_amount')
        #df_money_all=FEsingle.InputChgSum(df_money_all,5,'lg_amount')
        #df_money_all=FEsingle.InputChgSum(df_money_all,5,'net_mf_amount')

        #df_money_all=FEsingle.InputChgSum(df_money_all,12,'sm_amount')
        #df_money_all=FEsingle.InputChgSum(df_money_all,12,'lg_amount')
        #df_money_all=FEsingle.InputChgSum(df_money_all,12,'net_mf_amount')

        #df_money_all=FEsingle.InputChgSum(df_money_all,25,'sm_amount')
        #df_money_all=FEsingle.InputChgSum(df_money_all,25,'lg_amount')
        #df_money_all=FEsingle.InputChgSum(df_money_all,25,'net_mf_amount')

        #print(df_money_all)

        #df_data,_=FEsingle.DayFeatureToAll(df_data,name='pct_chg',method='std')
        #df_data,_=FEsingle.DayFeatureToAll(df_data,name='pct_chg',method='mean')
        ##df_data.to_csv('testsee1120.csv')
        #print(df_data)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_money_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        df_all['dayofweek']=pd.to_datetime(df_all['trade_date'],format='%Y%m%d')
        df_all['dayofweek']=df_all['dayofweek'].dt.dayofweek

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

        df_all=FEsingle.PredictDaysTrend(df_all,5)

        #df_all['real_open']=df_all['adj_factor']*df_all['open']
        #===================================================================================================================================#
        #df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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
        #df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,5,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,12,'max')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,5,'max')

        #df_all,_=FEsingle.HighLowRange(df_all,5)
        #df_all,_=FEsingle.HighLowRange(df_all,12)
        df_all,_=FEsingle.HighLowRange(df_all,25)    

        #df_all['25_pct_rank_min_diff']=df_all['25_pct_rank_min']-df_all['12_pct_rank_min']
        #df_all['12_pct_rank_min_diff']=df_all['12_pct_rank_min']-df_all['5_pct_rank_min']

        #df_all['25_pct_rank_max_diff']=df_all['25_pct_rank_max']-df_all['12_pct_rank_max']
        #df_all['12_pct_rank_max_diff']=df_all['12_pct_rank_max']-df_all['5_pct_rank_max']

        #df_all['25_pct_Rangerank_diff']=df_all['25_pct_Rangerank']-df_all['12_pct_Rangerank']
        #df_all['12_pct_Rangerank_diff']=df_all['12_pct_Rangerank']-df_all['5_pct_Rangerank']

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

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

        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        
        df_all=FEsingle.PctChgAbsSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,3)
        #df_all=FEsingle.PctChgSumRank(df_all,6)
        #df_all=FEsingle.PctChgSumRank(df_all,12)
        #df_all=FEsingle.PctChgSumRank(df_all,24)

        df_all=FEsingle.PctChgSum(df_all,3)
        #df_all=FEsingle.PctChgSum(df_all,6)
        #df_all=FEsingle.PctChgSum(df_all,12)
        #df_all=FEsingle.PctChgSum(df_all,24)

        #df_all['chg_rank_24_diff']=df_all['chg_rank_24']-df_all['chg_rank_12']
        #df_all['chg_rank_12_diff']=df_all['chg_rank_12']-df_all['chg_rank_6']
        #df_all['chg_rank_6_diff']=df_all['chg_rank_6']-df_all['chg_rank_3']

        #df_all['pct_chg_24_diff']=df_all['pct_chg_24']-df_all['pct_chg_12']
        #df_all['pct_chg_12_diff']=df_all['pct_chg_12']-df_all['pct_chg_6']
        #df_all['pct_chg_6_diff']=df_all['pct_chg_6']-df_all['pct_chg_3']


        #df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],2)

        ##df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],2)
        ##df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)


        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>2]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
        df_all=df_all[df_all['total_mv_rank']<6]
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

class FEfast_a41e(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        #df_money_all=pd.read_csv(DataSetName[3],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        #df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        #df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        #df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        #df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        #df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        #df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        #df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)


        #print(df_money_all)

        #df_data,_=FEsingle.DayFeatureToAll(df_data,name='pct_chg',method='std')
        #df_data,_=FEsingle.DayFeatureToAll(df_data,name='pct_chg',method='mean')


        #df_data['pct_chg_DayFeatureToAll_std_1']=df_data.groupby('ts_code')['pct_chg_DayFeatureToAll_std'].shift(1)
        #df_data['pct_chg_DayFeatureToAll_mean_1']=df_data.groupby('ts_code')['pct_chg_DayFeatureToAll_mean'].shift(1)
        #df_data['pct_chg_DayFeatureToAll_std_2']=df_data.groupby('ts_code')['pct_chg_DayFeatureToAll_std'].shift(2)
        #df_data['pct_chg_DayFeatureToAll_mean_2']=df_data.groupby('ts_code')['pct_chg_DayFeatureToAll_mean'].shift(2)

        #将空白的地方填满
        df_long_all['pe'].fillna(999,inplace=True)
        df_long_all['pb'].fillna(99,inplace=True)
        df_long_all['ps_ttm'].fillna(99,inplace=True)
        df_long_all['dv_ttm'].fillna(0,inplace=True)

        #df_long_all.to_csv('testsee1120.csv')
        print(df_long_all)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_money_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all.drop(['turnover_rate','volume_ratio'],axis=1,inplace=True)
        #df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)


        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        df_all['dayofweek']=pd.to_datetime(df_all['trade_date'],format='%Y%m%d')
        df_all['dayofweek']=df_all['dayofweek'].dt.dayofweek

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
        #df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

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

        df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        df_all['pe_rank']=df_all.groupby('ts_code')['pe_rank'].shift(1)
        df_all['dv_ttm']=df_all.groupby('trade_date')['dv_ttm'].rank(pct=True)
        df_all['dv_ttm']=df_all.groupby('ts_code')['dv_ttm'].shift(1)
        
        #===================================================================================================================================#

        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'min')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,12,'min')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,5,'min')
        df_all,_=FEsingle.CloseWithHighLow(df_all,25,'max')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,12,'max')
        #df_all,_=FEsingle.CloseWithHighLow(df_all,5,'max')

        #df_all,_=FEsingle.HighLowRange(df_all,5)
        #df_all,_=FEsingle.HighLowRange(df_all,12)
        df_all,_=FEsingle.HighLowRange(df_all,25)    

        #df_all['25_pct_rank_min_diff']=df_all['25_pct_rank_min']-df_all['12_pct_rank_min']
        #df_all['12_pct_rank_min_diff']=df_all['12_pct_rank_min']-df_all['5_pct_rank_min']

        #df_all['25_pct_rank_max_diff']=df_all['25_pct_rank_max']-df_all['12_pct_rank_max']
        #df_all['12_pct_rank_max_diff']=df_all['12_pct_rank_max']-df_all['5_pct_rank_max']

        #df_all['25_pct_Rangerank_diff']=df_all['25_pct_Rangerank']-df_all['12_pct_Rangerank']
        #df_all['12_pct_Rangerank_diff']=df_all['12_pct_Rangerank']-df_all['5_pct_Rangerank']

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

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

        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        
        df_all=FEsingle.PctChgAbsSumRank(df_all,6)
        df_all=FEsingle.PctChgSumRank(df_all,3)
        #df_all=FEsingle.PctChgSumRank(df_all,6)
        #df_all=FEsingle.PctChgSumRank(df_all,12)
        #df_all=FEsingle.PctChgSumRank(df_all,24)

        df_all=FEsingle.PctChgSum(df_all,3)
        #df_all=FEsingle.PctChgSum(df_all,6)
        #df_all=FEsingle.PctChgSum(df_all,12)
        #df_all=FEsingle.PctChgSum(df_all,24)

        #df_all['chg_rank_24_diff']=df_all['chg_rank_24']-df_all['chg_rank_12']
        #df_all['chg_rank_12_diff']=df_all['chg_rank_12']-df_all['chg_rank_6']
        #df_all['chg_rank_6_diff']=df_all['chg_rank_6']-df_all['chg_rank_3']

        #df_all['pct_chg_24_diff']=df_all['pct_chg_24']-df_all['pct_chg_12']
        #df_all['pct_chg_12_diff']=df_all['pct_chg_12']-df_all['pct_chg_6']
        #df_all['pct_chg_6_diff']=df_all['pct_chg_6']-df_all['pct_chg_3']


        #df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r'],1)

        #df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],2)

        ##df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],2)
        ##df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs','pe'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all=FEsingle.PredictDaysTrend(df_all,5)


        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>2]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
        df_all=df_all[df_all['total_mv_rank']<6]
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

class FEfast_b02(FEbase):
    #这个版本变为3天预测
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_limit_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        #df_money_all=pd.read_csv(DataSetName[3],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[4],index_col=0,header=0)

        #df_money_all.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        #df_money_all.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        #df_money_all.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        #df_money_all['sm_amount']=df_money_all['buy_sm_amount']-df_money_all['sell_sm_amount']
        #df_money_all['lg_amount']=df_money_all['buy_lg_amount']-df_money_all['sell_lg_amount']


        #df_money_all.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        #df_money_all.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        #print(df_money_all)

        #df_data,_=FEsingle.DayFeatureToAll(df_data,name='pct_chg',method='std')
        #df_data,_=FEsingle.DayFeatureToAll(df_data,name='pct_chg',method='mean')
        ##df_data.to_csv('testsee1120.csv')
        #print(df_data)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_money_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        #df_all.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df_all['limit_percent']=df_all['down_limit']/df_all['up_limit']

        #是否st或其他
        df_all['st_or_otherwrong']=0
        df_all.loc[(df_all['limit_percent']<0.85) & (0.58<df_all['limit_percent']),'st_or_otherwrong']=1

        df_all.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        df_all['dayofweek']=pd.to_datetime(df_all['trade_date'],format='%Y%m%d')
        df_all['dayofweek']=df_all['dayofweek'].dt.dayofweek

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

        df_all=FEsingle.PredictDaysReal5day(df_all,5)

        #df_all['real_open']=df_all['adj_factor']*df_all['open']
        #===================================================================================================================================#
        #df_all['real_price_pos']=df_all.groupby('ts_code')['real_price'].rolling(20).apply(lambda x: rollingRankSciPyB(x)).reset_index(0,drop=True)

        df_all['total_mv_rank']=df_all.groupby('trade_date')['total_mv'].rank(pct=True)
        #df_all['total_mv_rank']=df_all.groupby('ts_code')['total_mv_rank'].shift(1)
        #df_all['total_mv_rank']=df_all['total_mv_rank']*19.9//1


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

        df_all,_=FEsingle.CloseWithHighLow_self(df_all,25,'min')
        df_all,_=FEsingle.CloseWithHighLow_self(df_all,12,'min')
        df_all,_=FEsingle.CloseWithHighLow_self(df_all,5,'min')
        df_all,_=FEsingle.CloseWithHighLow_self(df_all,25,'max')
        df_all,_=FEsingle.CloseWithHighLow_self(df_all,12,'max')
        df_all,_=FEsingle.CloseWithHighLow_self(df_all,5,'max')

        df_all,_=FEsingle.HighLowRange_self(df_all,5)
        df_all,_=FEsingle.HighLowRange_self(df_all,12)
        df_all,_=FEsingle.HighLowRange_self(df_all,25)    

        #df_all['25_pct_rank_min_diff']=df_all['25_pct_rank_min']-df_all['12_pct_rank_min']
        #df_all['12_pct_rank_min_diff']=df_all['12_pct_rank_min']-df_all['5_pct_rank_min']

        #df_all['25_pct_rank_max_diff']=df_all['25_pct_rank_max']-df_all['12_pct_rank_max']
        #df_all['12_pct_rank_max_diff']=df_all['12_pct_rank_max']-df_all['5_pct_rank_max']

        #df_all['25_pct_Rangerank_diff']=df_all['25_pct_Rangerank']-df_all['12_pct_Rangerank']
        #df_all['12_pct_Rangerank_diff']=df_all['12_pct_Rangerank']-df_all['5_pct_Rangerank']

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
        #df_all['mvadj']=1
        #df_all.loc[df_all['total_mv_rank']<11,'mvadj']=0.9
        #df_all.loc[df_all['total_mv_rank']<7,'mvadj']=0.85
        #df_all.loc[df_all['total_mv_rank']<4,'mvadj']=0.6
        #df_all.loc[df_all['total_mv_rank']<2,'mvadj']=0.45
        #df_all.loc[df_all['total_mv_rank']<1,'mvadj']=0.35

        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        #df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        #df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2

        df_all['pct_chg_abs']=df_all['pct_chg'].abs()
        #df_all['pct_chg_abs_rank']=df_all.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        
        df_all=FEsingle.PctChgAbsSum_self(df_all,6)
        df_all=FEsingle.PctChgSum_self(df_all,3)
        df_all=FEsingle.PctChgSum_self(df_all,6)
        df_all=FEsingle.PctChgSum_self(df_all,12)
        df_all=FEsingle.PctChgSum_self(df_all,24)

        #df_all=FEsingle.PctChgSum(df_all,3)
        #df_all=FEsingle.PctChgSum(df_all,6)
        #df_all=FEsingle.PctChgSum(df_all,12)
        #df_all=FEsingle.PctChgSum(df_all,24)

        #df_all['chg_rank_24_diff']=df_all['chg_rank_24']-df_all['chg_rank_12']
        #df_all['chg_rank_12_diff']=df_all['chg_rank_12']-df_all['chg_rank_6']
        #df_all['chg_rank_6_diff']=df_all['chg_rank_6']-df_all['chg_rank_3']

        #df_all['pct_chg_24_diff']=df_all['pct_chg_24']-df_all['pct_chg_12']
        #df_all['pct_chg_12_diff']=df_all['pct_chg_12']-df_all['pct_chg_6']
        #df_all['pct_chg_6_diff']=df_all['pct_chg_6']-df_all['pct_chg_3']


        #df_all=FEsingle.AmountChgRank(df_all,12)
        #df_all=FEsingle.AmountChgRank(df_all,30)   

        #计算三种比例rank
        dolist=['open','high','low']

        df_all['pct_chg_r']=df_all['pct_chg']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            #df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            #df_all[curc]=df_all[curc]*9.9//2


        df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],1)
        #df_all=FEsingle.OldFeaturesRank(df_all,['sm_amount','lg_amount','net_mf_amount'],2)

        ##df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],2)
        ##df_all=FEsingle.OldFeaturesRank(df_all,['open','high','low','pct_chg_r','pst_amount_rank_12'],3)


        df_all.drop(['pre_close','adj_factor','total_mv','pb','circ_mv','pct_chg_abs'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        df_all.dropna(axis=0,how='any',inplace=True)


        #df_all['tomorrow_chg_rank'] = np.random.randint(0, 10, df_all.shape[0])

        #df_all.drop(['mvadj'],axis=1,inplace=True)

        df_all.drop(['pct_chg'],axis=1,inplace=True)

        #删除股价过低的票
        df_all=df_all[df_all['close']>2]
        #df_all=df_all[df_all['8_pct_rank_min']>0.1]
        #df_all=df_all[df_all['25_pct_rank_max']>0.1]
        
        df_all=df_all[df_all['total_mv_rank']<6]
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

class FEsingle:
    def CloseWithHighLow(df_all,days,minmax='min',intflag=False,standardflag=False,trainflag=False):
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

        if(standardflag):

            stringdisplay2=stringdisplay+'_Standard'
            stringdisplaysave=stringdisplay2+'.pkl'
            num_data=df_all[['30_pct']]
            
            if(trainflag):
                scaler=StandardScaler()
                num_data=scaler.fit_transform(num_data.values)
                joblib.dump(scaler,stringdisplaysave)
            else:
                scaler2=joblib.load(stringdisplaysave)
                num_data=scaler2.transform(num_data.values)

            df_all[stringdisplay2]=num_data

        df_all[stringdisplay]=df_all.groupby('trade_date')['30_pct'].rank(pct=True)

        df_all.drop(['30_pct','real_price_30min'],axis=1,inplace=True)
        if(intflag):
            df_all=FEsingle.changerank_line(df_all,stringdisplay)

        return df_all,stringdisplay

    def CloseWithHighLow_self(df_all,days,minmax='min',intflag=False,standardflag=False,trainflag=False):
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
        df_all[stringdisplay]=(100*(df_all['real_price']+0.001-df_all['real_price_30min']))/df_all['real_price_30min']

        #df_all[stringdisplay]=df_all.groupby('trade_date')['30_pct'].rank(pct=True)

        df_all.drop(['real_price_30min'],axis=1,inplace=True)
        if(intflag):
            df_all=FEsingle.changerank_line(df_all,stringdisplay)

        return df_all,stringdisplay

    def HighLowRange(df_all,days,intflag=False):
        #输入几日和最高或最低返回排名
        #30日最低比值

        stringdisplay=str(days)+'_pct_Rangerank'

        xxx=df_all.groupby('ts_code')['real_price'].rolling(days).min().reset_index()     
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)       

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')


        xxx=df_all.groupby('ts_code')['real_price'].rolling(days).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)       

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(100*(df_all['real_price_30max']+0.001-df_all['real_price_30min']))/df_all['real_price_30min']
        df_all[stringdisplay]=df_all.groupby('trade_date')['30_pct'].rank(pct=True)

        df_all.drop(['30_pct','real_price_30min','real_price_30max'],axis=1,inplace=True)
        if(intflag):
            df_all=FEsingle.changerank_line(df_all,stringdisplay)

        return df_all,stringdisplay

    def HighLowRange_self(df_all,days,intflag=False):
        #输入几日和最高或最低返回排名
        #30日最低比值

        stringdisplay=str(days)+'_pct_Rangerank'

        xxx=df_all.groupby('ts_code')['real_price'].rolling(days).min().reset_index()     
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)       

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')


        xxx=df_all.groupby('ts_code')['real_price'].rolling(days).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)       

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all[stringdisplay]=(100*(df_all['real_price_30max']+0.001-df_all['real_price_30min']))/df_all['real_price_30min']
        #df_all[stringdisplay]=df_all.groupby('trade_date')['30_pct'].rank(pct=True)

        df_all.drop(['real_price_30min','real_price_30max'],axis=1,inplace=True)
        if(intflag):
            df_all=FEsingle.changerank_line(df_all,stringdisplay)

        return df_all,stringdisplay

    def HighLowRangeReal(df_all,days,intflag=False):
        #输入几日和最高或最低返回排名
        #30日最低比值

        stringdisplay=str(days)+'_pct_RangerankReal'

        xxx=df_all.groupby('ts_code')['real_price'].rolling(days).min().reset_index()     
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)       

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')


        xxx=df_all.groupby('ts_code')['real_price'].rolling(days).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)       

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(100*(df_all['real_price_30max']+0.001-df_all['real_price_30min']))/df_all['real_price_30min']
        df_all[stringdisplay]=df_all['30_pct']

        df_all.drop(['30_pct','real_price_30min','real_price_30max'],axis=1,inplace=True)
        if(intflag):
            df_all=FEsingle.changerank_line(df_all,stringdisplay)

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

    def DayFeatureToAll(df_input,name,method='mean'):
        #输入需要对哪列进行处理，以及处理的方法
        #输出自动添加到对于df后方

        stringdisplay=name+'_DayFeatureToAll_'+method

        dftest=df_input.groupby(df_input['trade_date'])[name].agg(method)
        dftest2=pd.DataFrame({stringdisplay:dftest}).reset_index()
        df_input=pd.merge(df_input, dftest2, how='left', on=['trade_date'])

        return df_input,stringdisplay

    def changerank_line(df_all,b):
        df_all[b]=df_all[b]*19.9//2
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
        #df_all['tomorrow_chg']=df_all['tomorrow_chg']*df_all['mvadj']
        #df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1

        return df_all

    def PredictDaysTrend(df_all,days):

        ##明日幅度


        nextstart=df_all.groupby('ts_code')['real_price'].shift(0)
        nextnstart=df_all.groupby('ts_code')['real_price'].shift(0-days)

        #df_all['real_open2']=nextnstart
        #df_all['real_open1']=nextstart
        #df_all['real_open0']=df_all['real_open']

        df_all['tomorrow_chg']=((nextnstart-nextstart)/nextstart)*100
        #df_all['tomorrow_chg']=df_all['tomorrow_chg']*df_all['mvadj']
        #df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*19.9//1
        #df_all.loc[df_all['tomorrow_chg_rank']>28,'tomorrow_chg_rank']=30
        #df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']//3
        #df_all.loc[df_all['tomorrow_chg_rank']==9,'tomorrow_chg_rank']=8
        #df_all.loc[df_all['tomorrow_chg_rank']==10,'tomorrow_chg_rank']=9
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

    def PredictDaysReal5day(df_all,days):

        ##明日幅度
        #Y=[-9.34,-5.48,-4.2,-3.4,-2.7,-2.3,-1.86,-1.47,-1.09,-0.74,-0.38,0,0.398,0.838,1.35,1.96,2.74,3.81,5.58,10.77]

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
        df_all.loc[df_all['tomorrow_chg']>-9.34,'tomorrow_chg_rank']=1
        df_all.loc[df_all['tomorrow_chg']>-5.48,'tomorrow_chg_rank']=2
        df_all.loc[df_all['tomorrow_chg']>-4.2,'tomorrow_chg_rank']=3
        df_all.loc[df_all['tomorrow_chg']>-3.4,'tomorrow_chg_rank']=4
        df_all.loc[df_all['tomorrow_chg']>-2.7,'tomorrow_chg_rank']=5
        df_all.loc[df_all['tomorrow_chg']>-2.3,'tomorrow_chg_rank']=6
        df_all.loc[df_all['tomorrow_chg']>-1.86,'tomorrow_chg_rank']=7
        df_all.loc[df_all['tomorrow_chg']>-1.47,'tomorrow_chg_rank']=8
        df_all.loc[df_all['tomorrow_chg']>-1.09,'tomorrow_chg_rank']=9
        df_all.loc[df_all['tomorrow_chg']>-0.74,'tomorrow_chg_rank']=10
        df_all.loc[df_all['tomorrow_chg']>-0.38,'tomorrow_chg_rank']=11
        df_all.loc[df_all['tomorrow_chg']>0.398,'tomorrow_chg_rank']=12
        df_all.loc[df_all['tomorrow_chg']>0.838,'tomorrow_chg_rank']=13
        df_all.loc[df_all['tomorrow_chg']>1.35,'tomorrow_chg_rank']=14
        df_all.loc[df_all['tomorrow_chg']>1.96,'tomorrow_chg_rank']=15
        df_all.loc[df_all['tomorrow_chg']>2.74,'tomorrow_chg_rank']=16
        df_all.loc[df_all['tomorrow_chg']>3.81,'tomorrow_chg_rank']=17
        df_all.loc[df_all['tomorrow_chg']>5.58,'tomorrow_chg_rank']=18
        df_all.loc[df_all['tomorrow_chg']>10.77,'tomorrow_chg_rank']=19
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

    def PctChgSumRank(df_all,days,intflag=False):

        bufferbak='_'+str(days)
        stringdisplay='chg_rank_'+str(days)

        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(days).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        df_all[stringdisplay]=df_all.groupby('trade_date')[stringdisplay].rank(pct=True)
        if(intflag):
            df_all=FEsingle.changerank_line(df_all,stringdisplay)

        return df_all
    def PctChgSum(df_all,days,intflag=False):

        bufferbak='_'+str(days)
        stringdisplay='pct_chg_'+str(days)

        xxx=df_all.groupby('ts_code')['pct_chg'].rolling(days).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        #df_all[stringdisplay]=df_all.groupby('trade_date')[stringdisplay].rank(pct=True)
        if(intflag):
            df_all=FEsingle.changerank_line(df_all,stringdisplay)

        return df_all
    def PctChgAbsSumRank(df_all,days,intflag=False):

        bufferbak='_'+str(days)
        stringdisplay='pct_chg_abs_'+str(days)

        xxx=df_all.groupby('ts_code')['pct_chg_abs'].rolling(days).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        df_all[stringdisplay]=df_all.groupby('trade_date')[stringdisplay].rank(pct=True)
        if(intflag):
            df_all=FEsingle.changerank_line(df_all,stringdisplay)

        return df_all

    def PctChgAbsSum_self(df_all,days,intflag=False):

        bufferbak='_'+str(days)
        stringdisplay='pct_chg_abs_'+str(days)

        xxx=df_all.groupby('ts_code')['pct_chg_abs'].rolling(days).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        #df_all[stringdisplay]=df_all.groupby('trade_date')[stringdisplay].rank(pct=True)
        if(intflag):
            df_all=FEsingle.changerank_line(df_all,stringdisplay)

        return df_all
    def PctChgSum_self(df_all,days,intflag=False):

        bufferbak='_'+str(days)
        stringdisplay='chg_rank_'+str(days)

        xxx=df_all.groupby('ts_code')['pct_chg'].rolling(days).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        #df_all[stringdisplay]=df_all.groupby('trade_date')[stringdisplay].rank(pct=True)
        if(intflag):
            df_all=FEsingle.changerank_line(df_all,stringdisplay)

        return df_all

    def InputChgSum(df_all,days,sumlinename,intflag=False):

        bufferbak='_'+str(days)
        stringdisplay=sumlinename+'_'+str(days)

        xxx=df_all.groupby('ts_code')[sumlinename].rolling(days).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        #df_all[stringdisplay]=df_all.groupby('trade_date')[stringdisplay].rank(pct=True)
        if(intflag):
            df_all=FEsingle.changerank_line(df_all,stringdisplay)

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

    def AmountChgRank(df_all,days,intflag=False):

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
        if(intflag):
            df_all=FEsingle.changerank_line(df_all,stringdisplay)

        #df_all.drop(['pst_amount'],axis=1,inplace=True)
        return df_all

    def MoneyflowChgRank(df_all,days,intflag=False):

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
        if(intflag):
            df_all=FEsingle.changerank_line(df_all,stringdisplay)

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

#一些lambda 函数
def rollingRankArgSort(array):
    return array.size - array.argsort().argsort()[:-1]

def rollingRankArgSortBack(array):
    return array.argsort().argsort()[-1]


def rollingRankSciPy(array):
     return array.size - sc.stats.rankdata(array,method = 'ordinal')[0]

def rollingRankSciPyB(array):
     return sc.stats.rankdata(array,method = 'ordinal')[-1]
