#coding=utf-8

import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import tushare as ts
import os



from numpy import *

plt.ioff()
#matplotlib.use('TkAgg')
#plt.ion()


class Display(object):
    """description of class"""

    def __init__(self):
        pass

    def show_all_rate_ens_plus_plus(self,path):

        new_train_times=4

        #Y=[-12,-6,-3,-2,-1,0,0,0,0,0,0,1,1,1,1,1,2,3,7,14]
        Y=[-9.34,-5.48,-4.2,-3.4,-2.7,-2.3,-1.86,-1.47,-1.09,-0.74,-0.38,0,0.398,0.838,1.35,1.96,2.74,3.81,5.58,10.77]
        #Y=[-9.34,-5.48,-4.2,-3.4,-2.7,-2.3,-1.86,-1.47,-1.09,-0.74,-0.38,0,0.398,0.838,1.35,1.96,2.74,3.81,5.58,10.77]
        #Y=[-5,-5,-5,-5,-5,-4,-4,0,0,0,0,0,0,4,5,5,5,5,5,5]
        #Y=[-12,-6,-3,-2,-1,1,2,3,6,12]
        #Y=[0,0,0,0,0,0,0,0,0,100]
        #Y=[-8,-4,-3,-2,-1,1,2,3,4,8]
        #Y=[-12,0,0,0,0,0,0,0,0,12]
        #Y=[-12,-7,-3,-2,-1,1,2,5,10,18]
        #Y=[-3,-2,-1,-0.5,0,0,0.5,1,2,3]
        #Y=[3,2,1,0.5,0,0,-0.5,-1,-2,-3]
        #Y=[-12,-8,-6,-4,-3,0,0,0,0,0]
        #Y=[-12,-8,-3,-2,-1,1,2,3,10,18]
        #Y=[-12,0,0,0,0,0,0,0,8,18]
        #Y2=[-12,-8,-3,-2,-1,1,2,3,6,12]
        #Y=[-12,-7,-3,-2,-1,1,0,1,4,7]

        codechoice=20
        dayrange=5

        all_csv_path=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)       
        all_csv_path=all_csv_path.loc[:,['ts_code','trade_date','pct_chg','pre_close','open','amount']]
        all_csv_path['pct_chg']=all_csv_path['pct_chg'].astype('float64')

        #all_csv_path['open']=all_csv_path['open'].astype('float64')
        #all_csv_path['next_open']=all_csv_path.groupby('ts_code')['open'].shift(0)
        #all_csv_path['next_open2']=all_csv_path.groupby('ts_code')['open'].shift(-1)
        #all_csv_path['pct_chg']=((all_csv_path['next_open2']-all_csv_path['next_open'])/(all_csv_path['next_open']+0.00001))*100
        #all_csv_path['open']=all_csv_path['pre_close'].astype('float64')
        #all_csv_path['next_open']=all_csv_path.groupby('ts_code')['open'].shift(0)
        #all_csv_path['next_open2']=all_csv_path.groupby('ts_code')['open'].shift(-1)
        #all_csv_path['pct_chg']=((all_csv_path['next_open2']-all_csv_path['next_open'])/(all_csv_path['next_open']+0.00001))*100


        #all_csv_path['pct_chg']=all_csv_path.groupby('ts_code')['pct_chg'].shift(-1)

        #提取几天的pctchg
        for i in range(dayrange):
            shifti=-i-1
            tm=all_csv_path.groupby('ts_code')['pct_chg'].shift(shifti)
            tm.fillna(0, inplace=True)
            stringi="pct_chg_next_"+str(i+1)
            all_csv_path[stringi]=tm

        showsource_list=[]
        for counter in range(new_train_times):
            path_new=os.path.splitext(path)[0]+str(counter)+".csv"

            ss=pd.read_csv(path_new,index_col=0,header=0)
            #if(counter<4):
            if(counter<8):
                #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]+ss['10']*Y[10]+ss['11']*Y[11]+ss['12']*Y[12]+ss['13']*Y[13]+ss['14']*Y[14]
                ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]+ss['10']*Y[10]+ss['11']*Y[11]+ss['12']*Y[12]+ss['13']*Y[13]+ss['14']*Y[14]+ss['15']*Y[15]+ss['16']*Y[16]+ss['17']*Y[17]+ss['18']*Y[18]+ss['19']*Y[19]

                #ss['mix']=(ss['0']**0.5)*Y[0]+(ss['1']**0.5)*Y[1]+(ss['2']**0.5)*Y[2]+(ss['3']**0.5)*Y[3]+(ss['4']**0.5)*Y[4]+(ss['5']**0.5)*Y[5]+(ss['6']**0.5)*Y[6]+(ss['7']**0.5)*Y[7]+(ss['8']**0.5)*Y[8]+(ss['9']**0.5)*Y[9]
                #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]
                #ss['mix']=(ss['0']**2)*Y[0]+(ss['1']**2)*Y[1]+(ss['2']**2)*Y[2]+(ss['3']**2)*Y[3]+(ss['4']**2)*Y[4]+(ss['5']**2)*Y[5]+(ss['6']**2)*Y[6]+(ss['7']**2)*Y[7]+(ss['8']**2)*Y[8]+(ss['9']**2)*Y[9]
                #ss['mix']=(ss['9']/ss['0'])*8+(ss['8']/ss['1'])*4+(ss['7']/ss['2'])*2+(ss['6']/ss['3'])*1
                ss['mix']=ss.groupby('trade_date')['mix'].rank(ascending=True,pct=True,method='first')

            else:
                ss['mix']=ss['0']
                ss['mix']=ss.groupby('trade_date')['mix'].rank(ascending=True,pct=False,method='first')

            #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]
            #see=ss[ss['trade_date']==20201113]
            #see=see[see['ts_code']=='300208.SZ']
            #print(see['mix'])
            showsource_list.append(ss)
        
        #showsource_list[0]['mix']=showsource_list[0].groupby('trade_date')['mix'].rank(ascending=False)
        showsource=showsource_list[0]
        print(showsource_list[0])
        for counter in range(new_train_times):

            if counter==0:
                showsource['sum0']=showsource['0']
                showsource['sum19']=showsource['19']
                continue
            #showsource['mix']=showsource['mix']+showsource_list[counter].groupby('trade_date')['mix'].rank(ascending=False)

            mergedf=showsource_list[counter][['ts_code','trade_date','mix']]
            print(mergedf)
            showsource=pd.merge(showsource,mergedf,on=['ts_code','trade_date'],suffixes=('','_new'))
            
            #showsource=pd.merge(showsource, showsource_list[counter].iloc[:, ['mix']], how='left', on=['trade_date','ts_code'],suffixes=('mix','mix_2'))

            showsource['mix']=showsource['mix']+showsource['mix_new']
            showsource.drop(['mix_new'],axis=1,inplace=True)

            showsource['sum0']=showsource['sum0']+showsource_list[counter]['0']
            showsource['sum19']=showsource['sum19']+showsource_list[counter]['19']
            sfasfd=12
        

        showsource=pd.merge(showsource, all_csv_path, how='left', on=['ts_code','trade_date'])

        print(showsource['mix'])
        #databuffer=showsource['trade_date'].unique()

        #showsource2=showsource[showsource['trade_date']>20201112]
        #showsource2=showsource2[showsource2['trade_date']<20201120]
        #showsource2.to_csv('seefef2.csv')

        #剔除无法买入的涨停股
        showsource['high_stop']=0
        showsource.loc[showsource['pct_chg']>9.4,'high_stop']=1
        showsource.loc[(showsource['pct_chg']<5.2) & (4.8<showsource['pct_chg']),'high_stop']=1
        showsource.loc[showsource['pre_close']<1.5,'high_stop']=1
        #showsource.loc[showsource['total_mv_rank']>17,'high_stop']=1
        #showsource.loc[showsource['sum0']<0.1,'high_stop']=1
        #删除未大于平均值的19
        #showsource.loc[showsource['sum0']<0.06,'high_stop']=1
        showsource=showsource[showsource['high_stop']==0]



        #showsource=showsource[showsource['total_mv_rank']>17]
        #showsource=showsource[showsource['total_mv_rank']>10]

        showsource['mix_rank']=showsource.groupby('trade_date')['mix'].rank(ascending=False,pct=False,method='first')
        print(showsource)
        showsource['next_chg']=showsource.groupby('ts_code')['tomorrow_chg'].shift(-1)
        showsource['mix_rank_real']=showsource.groupby('trade_date')['tomorrow_chg'].rank(ascending=False,pct=False,method='first')

        #showsource=showsource[showsource['trade_date']>20171230]
        #showsource=showsource[showsource['ts_code'].str.startswith('300')==False]

        databuffer=showsource['trade_date'].unique()

        bufferLL=[]
        
        index=0
        maxday=len(databuffer)

        all_csv_path['buyflag']=0
        all_csv_path=all_csv_path[all_csv_path['trade_date'].isin(databuffer)]

        for curdata in databuffer:
            #curday=databuffer[index]
            cur_show=showsource[showsource["trade_date"]==curdata]
            cur_show=cur_show[cur_show['high_stop']==0]
            #cur_show=cur_show[cur_show['amount']>10000]
            #if(curdata<20180101):
            #    continue
            cc=cur_show.sort_values(by="mix" , ascending=False)

            ##简易查看
            #dd=cc['tomorrow_chg'].mean()
            #ee=cc.head(10)
            #print(curdata)
            #print(ee['ts_code'])
            #ff=ee['tomorrow_chg'].mean()
            #gg=ff-dd
            #print(gg,end='')
            #print('   ',end='')
            #print(ff,end='')
            #print('   ',end='')
            #print(curdata)

            bufferL=[]
            for i in range(dayrange):
                stringi="pct_chg_next_"+str(i+1)
                nextlist=cc.head(codechoice)[stringi].values
                bufferL.append(nextlist)

            bufferLL.append(bufferL)

            index+=1

        #print(bufferLL)
        changer=[]
        index2=0
        times=codechoice*dayrange

        tendencyrange=30
        lastsumlist = np.zeros(tendencyrange)
        newtendlist=np.ones(dayrange)

        for curlist in bufferLL:
            sum=0
            sumtendency=0

            for i in range(dayrange):
                buferi=index2-i
                if buferi>=0:
                    tempsum=bufferLL[buferi][i].sum()/times
                    sum+=tempsum
                    sumtendency+=(newtendlist[i]*tempsum)

            for i in range(dayrange):
                if((dayrange-i)>1):
                    newtendlist[dayrange-i-1]=newtendlist[dayrange-i-2]
                else:
                    break

            if(lastsumlist.sum()>0):
                sum2=sum
                newtendlist[0]=1
            else:
                sum2=0
                newtendlist[0]=0

            #changer.append(sumtendency)
            changer.append(sum)
            print(sumtendency,end='')
            print('   ',end='')
            print(index2)

            for i in range(tendencyrange):
                if((tendencyrange-i)>1):
                    lastsumlist[tendencyrange-i-1]=lastsumlist[tendencyrange-i-2]
                else:
                    break
            lastsumlist[0]=sum


            index2+=1
        #print(changer)

        #days2,show=self.standard_show(changer,day_interval=1)
        days2,show=self.standard_show_Kelly_Criterion_new(changer,first_base_income=100000,day_interval=dayrange)
        
        self.show_screen(showsource)
        #showsource=showsource[showsource['trade_date']>20170301]
        showsource=showsource[showsource['trade_date']>20210301]
        #showsource=showsource[showsource['trade_date']<20170901]
        #showsource=showsource[showsource['mix_rank']<200]

        showsource.to_csv('seefef4.csv')
        return days2,show

    def show_all_rate_ens_plus_plus_plus(self,path):

        USE_OTHER_DATASOURCE=False
        IS_DESC=False

        new_train_times=4

        #Y=[-12,-6,-3,-2,-1,0,0,0,0,0,0,0,0,0,0,1,2,3,6,12]
        Y=[-9.34,-5.48,-4.2,-3.4,-2.7,-2.3,-1.86,-1.47,-1.09,-0.74,-0.38,0,0.398,0.838,1.35,1.96,2.74,3.81,5.58,10.77]
        #Y=[-9.34,-5.48,-4.2,-3.4,-2.7,-2.3,-1.86,-1.47,-1.09,-0.74,-0.38,0,0.398,0.838,1.35,1.96,2.74,3.81,5.58,10.77]
        #Y=[-5,-5,-5,-5,-5,-4,-4,0,0,0,0,0,0,4,5,5,5,5,5,5]
        #Y=[-12,-6,-3,-2,-1,1,2,3,6,12]

        codechoice=30
        dayrange=5

        all_csv_path=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)       
        all_csv_path=all_csv_path.loc[:,['ts_code','trade_date','pct_chg','pre_close','open','low','close','high','amount']]
        all_csv_path['pct_chg']=all_csv_path['pct_chg'].astype('float64')

        #all_adj=pd.read_csv('./Database/Daily_adj_factor.csv',index_col=0,header=0) 
        all_csv_path=all_csv_path[all_csv_path['trade_date']>20130101]
        all_csv_path_fhc=None

        if(USE_OTHER_DATASOURCE):
            all_csv_path_fhc=pd.read_csv('./qlibdataset333.csv',index_col=0,header=0)
            print(all_csv_path_fhc)
            dddd=1


        #all_csv_path=pd.merge(all_csv_path, all_adj, how='left', on=['ts_code','trade_date'])

        all_csv_path['open']=all_csv_path['open'].astype('float64')
        #all_csv_path['low']=all_csv_path['low'].astype('float64')
        all_csv_path['close']=all_csv_path['close'].astype('float64')
        #all_csv_path['high']=all_csv_path['high'].astype('float64')
        all_csv_path['pre_close']=all_csv_path['pre_close'].astype('float64')
        all_csv_path['open_pct_chg']=(all_csv_path['open']-all_csv_path['pre_close'])/all_csv_path['pre_close']
        all_csv_path['closeopen_pct_chg']=100*(all_csv_path['close']-all_csv_path['open'])/all_csv_path['open']
        #all_csv_path['low_pct_chg']=100*(all_csv_path['low']-all_csv_path['pre_close'])/all_csv_path['pre_close']
        #all_csv_path['low_pct_chg']=100*(all_csv_path['low']-all_csv_path['pre_close'])/all_csv_path['pre_close']


        #lowbuy=5
        #all_csv_path['lowclose_pct_chg']=100*(all_csv_path['close']-all_csv_path['pre_close']*(1+lowbuy/100))/(all_csv_path['pre_close']*(1+lowbuy/100))


        #all_csv_path['lowclose_pct_chg']=all_csv_path['low_pct_chg']
        #all_csv_path['lowclose_pct_chg']=all_csv_path[all_csv_path['lowclose_pct_chg']<lowbuy]



        ###用来测第一天开盘价买入策略
        ###all_csv_path['next_open_pct_chg']=all_csv_path.groupby('ts_code')['open_pct_chg'].shift(-1)
        all_csv_path['next_closeopen_pct_chg']=all_csv_path.groupby('ts_code')['closeopen_pct_chg'].shift(-1)
        ##all_csv_path['next_low_pct_chg']=all_csv_path.groupby('ts_code')['low_pct_chg'].shift(-1)
        ##all_csv_path['next_lowclose_pct_chg']=all_csv_path.groupby('ts_code')['lowclose_pct_chg'].shift(-1)
        all_csv_path['next_open_pct_chg']=all_csv_path.groupby('ts_code')['open_pct_chg'].shift(-1)

        #all_csv_path['open']=all_csv_path['open'].astype('float64')
        #all_csv_path['next_open']=all_csv_path.groupby('ts_code')['open'].shift(0)
        #all_csv_path['next_open2']=all_csv_path.groupby('ts_code')['open'].shift(-1)
        #all_csv_path['pct_chg']=((all_csv_path['next_open2']-all_csv_path['next_open'])/(all_csv_path['next_open']+0.00001))*100
        #all_csv_path['open']=all_csv_path['pre_close'].astype('float64')
        #all_csv_path['next_open']=all_csv_path.groupby('ts_code')['open'].shift(0)
        #all_csv_path['next_open2']=all_csv_path.groupby('ts_code')['open'].shift(-1)
        #all_csv_path['pct_chg']=((all_csv_path['next_open2']-all_csv_path['next_open'])/(all_csv_path['next_open']+0.00001))*100


        #all_csv_path['pct_chg']=all_csv_path.groupby('ts_code')['pct_chg'].shift(-1)

        #提取几天的pctchg
        for i in range(dayrange):
            shifti=-i-1
            tm=all_csv_path.groupby('ts_code')['pct_chg'].shift(shifti)
            tm.fillna(0, inplace=True)
            stringi="pct_chg_next_"+str(i+1)
            all_csv_path[stringi]=tm


        all_csv_path['next_buyflag2']=0
        ##all_csv_path['next_buyflag2']=0

        ##all_csv_path['pct_chg_next_1']=all_csv_path['next_lowclose_pct_chg']

        ##all_csv_path.loc[all_csv_path['next_open_pct_chg']<(lowbuy),'pct_chg_next_1']=all_csv_path['next_closeopen_pct_chg']

        all_csv_path['pct_chg_next_1']=all_csv_path['next_closeopen_pct_chg']
        all_csv_path.loc[all_csv_path['next_open_pct_chg']>9.5,'next_buyflag2']=1


        ##all_csv_path.loc[all_csv_path['next_low_pct_chg']>(lowbuy),'pct_chg_next_1']=0
        ##all_csv_path.loc[(all_csv_path['next_buyflag']>(9.5))&(all_csv_path['pct_chg_next_1']==0),'next_buyflag2']=1
        all_csv_path.loc[(all_csv_path['next_buyflag2']==1),'pct_chg_next_1']=0
        all_csv_path.loc[(all_csv_path['next_buyflag2']==1),'pct_chg_next_2']=0
        all_csv_path.loc[(all_csv_path['next_buyflag2']==1),'pct_chg_next_3']=0
        all_csv_path.loc[(all_csv_path['next_buyflag2']==1),'pct_chg_next_4']=0
        all_csv_path.loc[(all_csv_path['next_buyflag2']==1),'pct_chg_next_5']=0


        #all_csv_path.to_csv('testsee1113.csv')
        showsource=[]
        if(not USE_OTHER_DATASOURCE):

            showsource_list=[]
            for counter in range(new_train_times):
                path_new=os.path.splitext(path)[0]+str(counter)+".csv"

                ss=pd.read_csv(path_new,index_col=0,header=0)
                #if(counter<4):
                if(counter<8):
                    #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]+ss['10']*Y[10]+ss['11']*Y[11]+ss['12']*Y[12]+ss['13']*Y[13]+ss['14']*Y[14]
                    ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]+ss['10']*Y[10]+ss['11']*Y[11]+ss['12']*Y[12]+ss['13']*Y[13]+ss['14']*Y[14]+ss['15']*Y[15]+ss['16']*Y[16]+ss['17']*Y[17]+ss['18']*Y[18]+ss['19']*Y[19]

                    #ss['mix']=(ss['0']**0.5)*Y[0]+(ss['1']**0.5)*Y[1]+(ss['2']**0.5)*Y[2]+(ss['3']**0.5)*Y[3]+(ss['4']**0.5)*Y[4]+(ss['5']**0.5)*Y[5]+(ss['6']**0.5)*Y[6]+(ss['7']**0.5)*Y[7]+(ss['8']**0.5)*Y[8]+(ss['9']**0.5)*Y[9]
                    #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]
                    #ss['mix']=(ss['0']**2)*Y[0]+(ss['1']**2)*Y[1]+(ss['2']**2)*Y[2]+(ss['3']**2)*Y[3]+(ss['4']**2)*Y[4]+(ss['5']**2)*Y[5]+(ss['6']**2)*Y[6]+(ss['7']**2)*Y[7]+(ss['8']**2)*Y[8]+(ss['9']**2)*Y[9]
                    #ss['mix']=(ss['9']/ss['0'])*8+(ss['8']/ss['1'])*4+(ss['7']/ss['2'])*2+(ss['6']/ss['3'])*1
                    ss['mix']=ss.groupby('trade_date')['mix'].rank(ascending=True,pct=True,method='first')

                else:
                    ss['mix']=ss['0']
                    ss['mix']=ss.groupby('trade_date')['mix'].rank(ascending=True,pct=False,method='first')

                #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]
                #see=ss[ss['trade_date']==20201113]
                #see=see[see['ts_code']=='300208.SZ']
                #print(see['mix'])
                showsource_list.append(ss)
        
            #showsource_list[0]['mix']=showsource_list[0].groupby('trade_date')['mix'].rank(ascending=False)
            showsource=showsource_list[0]
            print(showsource_list[0])
            for counter in range(new_train_times):

                if counter==0:
                    showsource['sum0']=showsource['0']
                    showsource['sum19']=showsource['19']
                    continue
                #showsource['mix']=showsource['mix']+showsource_list[counter].groupby('trade_date')['mix'].rank(ascending=False)

                mergedf=showsource_list[counter][['ts_code','trade_date','mix']]
                print(mergedf)
                showsource=pd.merge(showsource,mergedf,on=['ts_code','trade_date'],suffixes=('','_new'))
            
                #showsource=pd.merge(showsource, showsource_list[counter].iloc[:, ['mix']], how='left', on=['trade_date','ts_code'],suffixes=('mix','mix_2'))

                showsource['mix']=showsource['mix']+showsource['mix_new']
                showsource.drop(['mix_new'],axis=1,inplace=True)

                showsource['sum0']=showsource['sum0']+showsource_list[counter]['0']
                showsource['sum19']=showsource['sum19']+showsource_list[counter]['19']

            showsource=pd.merge(showsource, all_csv_path, how='left', on=['ts_code','trade_date'])  
        
        else:

            showsource=pd.merge(all_csv_path, all_csv_path_fhc, how='left', on=['ts_code','trade_date'])

            showsource.dropna(axis=0,how='any',inplace=True)

            #showsource['mix']=showsource['pred']
            print(showsource)
            


        print(showsource['mix'])
        #databuffer=showsource['trade_date'].unique()

        #showsource2=showsource[showsource['trade_date']>20201112]
        #showsource2=showsource2[showsource2['trade_date']<20201120]
        #showsource.to_csv('seefef2.csv')

        #剔除无法买入的涨停股
        showsource['high_stop']=0
        showsource.loc[showsource['pct_chg']>9.4,'high_stop']=1
        showsource.loc[(showsource['pct_chg']<5.2) & (4.8<showsource['pct_chg']),'high_stop']=1
        showsource.loc[showsource['pre_close']<1.5,'high_stop']=1



        #showsource.loc[showsource['total_mv_rank']>17,'high_stop']=1
        #showsource.loc[showsource['sum0']<0.1,'high_stop']=1
        #删除未大于平均值的19
        #showsource.loc[showsource['sum0']<0.06,'high_stop']=1
        showsource=showsource[showsource['high_stop']==0]



        #showsource=showsource[showsource['total_mv_rank']>17]
        #showsource=showsource[showsource['total_mv_rank']>10]

        showsource['mix_rank']=showsource.groupby('trade_date')['mix'].rank(ascending=False,pct=False,method='first')
        print(showsource)
        if(not USE_OTHER_DATASOURCE):
            showsource['next_chg']=showsource.groupby('ts_code')['tomorrow_chg'].shift(-1)
            showsource['mix_rank_real']=showsource.groupby('trade_date')['tomorrow_chg'].rank(ascending=False,pct=False,method='first')

        #showsource=showsource[showsource['trade_date']>20171230]
        #showsource=showsource[showsource['ts_code'].str.startswith('300')==False]

        databuffer=showsource['trade_date'].unique()

        bufferLL=[]
        
        index=0
        maxday=len(databuffer)

        all_csv_path['buyflag']=0
        all_csv_path=all_csv_path[all_csv_path['trade_date'].isin(databuffer)]

        for curdata in databuffer:
            #curday=databuffer[index]
            cur_show=showsource[showsource["trade_date"]==curdata]
            cur_show=cur_show[cur_show['high_stop']==0]
            #cur_show=cur_show[cur_show['amount']>10000]
            #if(curdata<20180101):
            #    continue
            cc=cur_show.sort_values(by="mix" , ascending=False)

            ##简易查看
            #dd=cc['tomorrow_chg'].mean()
            #ee=cc.head(10)
            #print(curdata)
            #print(ee['ts_code'])
            #ff=ee['tomorrow_chg'].mean()
            #gg=ff-dd
            #print(gg,end='')
            #print('   ',end='')
            #print(ff,end='')
            #print('   ',end='')
            #print(curdata)

            bufferL=[]
            for i in range(dayrange):
                stringi="pct_chg_next_"+str(i+1)
                if(not IS_DESC):
                    nextlist=cc.head(codechoice)[stringi].values
                else:
                    nextlist=cc.tail(codechoice)[stringi].values
                bufferL.append(nextlist)

            bufferLL.append(bufferL)

            index+=1

        #print(bufferLL)
        changer=[]
        index2=0
        times=codechoice*dayrange

        for curlist in bufferLL:
            sum=0
            sumtendency=0

            for i in range(dayrange):
                buferi=index2-i
                if buferi>=0:
                    tempsum=bufferLL[buferi][i].sum()/times
                    sum+=tempsum

            changer.append(sum)

            index2+=1
        #print(changer)

        #days2,show=self.standard_show(changer,day_interval=1)
        days2,show=self.standard_show_Kelly_Criterion_new(changer,first_base_income=100000,day_interval=dayrange)
        
        if(True):
            self.show_screen(showsource)

            #showsource=showsource[showsource['trade_date']>20170301]
            #showsource=showsource[showsource['trade_date']>20210301]
            #showsource=showsource[showsource['trade_date']<20200801]
            #showsource=showsource[showsource['mix_rank']<200]

            #showsource.to_csv('seefef4.csv')
            #showsource=showsource.loc[:,['ts_code','trade_date','close','open','amount']]
            showsource=showsource.loc[:,['ts_code','trade_date','mix']]
            showsource.to_csv('zzzzfackdatapred.csv')

        else:
            self.changetoqlib(showsource)


        return databuffer,show

    def show_all_rate_ens_plus_plus_plus_day(self,path):

        new_train_times=4

        Y=[-12,-6,-3,-2,-1,0,0,0,0,0,0,0,0,0,0,0,0,1,2,5]
        #Y=[-9.34,-5.48,-4.2,-3.4,-2.7,-2.3,-1.86,-1.47,-1.09,-0.74,-0.38,0,0.398,0.838,1.35,1.96,2.74,3.81,5.58,10.77]
        #Y=[-9.34,-5.48,-4.2,-3.4,-2.7,-2.3,-1.86,-1.47,-1.09,-0.74,-0.38,0,0.398,0.838,1.35,1.96,2.74,3.81,5.58,10.77]
        #Y=[-5,-5,-5,-5,-5,-4,-4,0,0,0,0,0,0,4,5,5,5,5,5,5]
        #Y=[-12,-6,-3,-2,-1,1,2,3,6,12]


        codechoice=30
        dayrange=5

        all_csv_path=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)       
        all_csv_path=all_csv_path.loc[:,['ts_code','trade_date','pct_chg','pre_close','open','low','close','high','amount']]
        all_csv_path['pct_chg']=all_csv_path['pct_chg'].astype('float64')

        #all_adj=pd.read_csv('./Database/Daily_adj_factor.csv',index_col=0,header=0) 

        all_csv_path=all_csv_path[all_csv_path['trade_date']>20130101]

        #all_csv_path=pd.merge(all_csv_path, all_adj, how='left', on=['ts_code','trade_date'])

        all_csv_path['open']=all_csv_path['open'].astype('float64')
        #all_csv_path['low']=all_csv_path['low'].astype('float64')
        all_csv_path['close']=all_csv_path['close'].astype('float64')
        #all_csv_path['high']=all_csv_path['high'].astype('float64')
        all_csv_path['pre_close']=all_csv_path['pre_close'].astype('float64')
        all_csv_path['open_pct_chg']=(all_csv_path['open']-all_csv_path['pre_close'])/all_csv_path['pre_close']
        all_csv_path['closeopen_pct_chg']=100*(all_csv_path['close']-all_csv_path['open'])/all_csv_path['open']
        #all_csv_path['low_pct_chg']=100*(all_csv_path['low']-all_csv_path['pre_close'])/all_csv_path['pre_close']
        #all_csv_path['low_pct_chg']=100*(all_csv_path['low']-all_csv_path['pre_close'])/all_csv_path['pre_close']


        #lowbuy=5
        #all_csv_path['lowclose_pct_chg']=100*(all_csv_path['close']-all_csv_path['pre_close']*(1+lowbuy/100))/(all_csv_path['pre_close']*(1+lowbuy/100))


        #all_csv_path['lowclose_pct_chg']=all_csv_path['low_pct_chg']
        #all_csv_path['lowclose_pct_chg']=all_csv_path[all_csv_path['lowclose_pct_chg']<lowbuy]



        ###用来测第一天开盘价买入策略
        ###all_csv_path['next_open_pct_chg']=all_csv_path.groupby('ts_code')['open_pct_chg'].shift(-1)
        all_csv_path['next_closeopen_pct_chg']=all_csv_path.groupby('ts_code')['closeopen_pct_chg'].shift(-1)
        ##all_csv_path['next_low_pct_chg']=all_csv_path.groupby('ts_code')['low_pct_chg'].shift(-1)
        ##all_csv_path['next_lowclose_pct_chg']=all_csv_path.groupby('ts_code')['lowclose_pct_chg'].shift(-1)
        all_csv_path['next_open_pct_chg']=all_csv_path.groupby('ts_code')['open_pct_chg'].shift(-1)

        #all_csv_path['open']=all_csv_path['open'].astype('float64')
        #all_csv_path['next_open']=all_csv_path.groupby('ts_code')['open'].shift(0)
        #all_csv_path['next_open2']=all_csv_path.groupby('ts_code')['open'].shift(-1)
        #all_csv_path['pct_chg']=((all_csv_path['next_open2']-all_csv_path['next_open'])/(all_csv_path['next_open']+0.00001))*100
        #all_csv_path['open']=all_csv_path['pre_close'].astype('float64')
        #all_csv_path['next_open']=all_csv_path.groupby('ts_code')['open'].shift(0)
        #all_csv_path['next_open2']=all_csv_path.groupby('ts_code')['open'].shift(-1)
        #all_csv_path['pct_chg']=((all_csv_path['next_open2']-all_csv_path['next_open'])/(all_csv_path['next_open']+0.00001))*100


        #all_csv_path['pct_chg']=all_csv_path.groupby('ts_code')['pct_chg'].shift(-1)

        #提取几天的pctchg
        for i in range(dayrange):
            shifti=-i-1
            tm=all_csv_path.groupby('ts_code')['pct_chg'].shift(shifti)
            tm.fillna(0, inplace=True)
            stringi="pct_chg_next_"+str(i+1)
            all_csv_path[stringi]=tm


        all_csv_path['next_buyflag2']=0
        ##all_csv_path['next_buyflag2']=0

        ##all_csv_path['pct_chg_next_1']=all_csv_path['next_lowclose_pct_chg']

        ##all_csv_path.loc[all_csv_path['next_open_pct_chg']<(lowbuy),'pct_chg_next_1']=all_csv_path['next_closeopen_pct_chg']

        all_csv_path['pct_chg_next_1']=all_csv_path['next_closeopen_pct_chg']
        all_csv_path.loc[all_csv_path['next_open_pct_chg']>9.5,'next_buyflag2']=1


        ##all_csv_path.loc[all_csv_path['next_low_pct_chg']>(lowbuy),'pct_chg_next_1']=0
        ##all_csv_path.loc[(all_csv_path['next_buyflag']>(9.5))&(all_csv_path['pct_chg_next_1']==0),'next_buyflag2']=1
        all_csv_path.loc[(all_csv_path['next_buyflag2']==1),'pct_chg_next_1']=0
        all_csv_path.loc[(all_csv_path['next_buyflag2']==1),'pct_chg_next_2']=0
        all_csv_path.loc[(all_csv_path['next_buyflag2']==1),'pct_chg_next_3']=0
        all_csv_path.loc[(all_csv_path['next_buyflag2']==1),'pct_chg_next_4']=0
        all_csv_path.loc[(all_csv_path['next_buyflag2']==1),'pct_chg_next_5']=0


        #all_csv_path.to_csv('testsee1113.csv')

        showsource_list=[]
        for counter in range(new_train_times):
            path_new=os.path.splitext(path)[0]+str(counter)+".csv"

            ss=pd.read_csv(path_new,index_col=0,header=0)
            #if(counter<4):
            if(counter<8):
                #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]+ss['10']*Y[10]+ss['11']*Y[11]+ss['12']*Y[12]+ss['13']*Y[13]+ss['14']*Y[14]
                ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]+ss['10']*Y[10]+ss['11']*Y[11]+ss['12']*Y[12]+ss['13']*Y[13]+ss['14']*Y[14]+ss['15']*Y[15]+ss['16']*Y[16]+ss['17']*Y[17]+ss['18']*Y[18]+ss['19']*Y[19]

                #ss['mix']=(ss['0']**0.5)*Y[0]+(ss['1']**0.5)*Y[1]+(ss['2']**0.5)*Y[2]+(ss['3']**0.5)*Y[3]+(ss['4']**0.5)*Y[4]+(ss['5']**0.5)*Y[5]+(ss['6']**0.5)*Y[6]+(ss['7']**0.5)*Y[7]+(ss['8']**0.5)*Y[8]+(ss['9']**0.5)*Y[9]
                #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]
                #ss['mix']=(ss['0']**2)*Y[0]+(ss['1']**2)*Y[1]+(ss['2']**2)*Y[2]+(ss['3']**2)*Y[3]+(ss['4']**2)*Y[4]+(ss['5']**2)*Y[5]+(ss['6']**2)*Y[6]+(ss['7']**2)*Y[7]+(ss['8']**2)*Y[8]+(ss['9']**2)*Y[9]
                #ss['mix']=(ss['9']/ss['0'])*8+(ss['8']/ss['1'])*4+(ss['7']/ss['2'])*2+(ss['6']/ss['3'])*1
                ss['mix']=ss.groupby('trade_date')['mix'].rank(ascending=True,pct=True,method='first')

            else:
                ss['mix']=ss['0']
                ss['mix']=ss.groupby('trade_date')['mix'].rank(ascending=True,pct=False,method='first')

            #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]
            #see=ss[ss['trade_date']==20201113]
            #see=see[see['ts_code']=='300208.SZ']
            #print(see['mix'])
            showsource_list.append(ss)
        
        #showsource_list[0]['mix']=showsource_list[0].groupby('trade_date')['mix'].rank(ascending=False)
        showsource=showsource_list[0]
        print(showsource_list[0])
        for counter in range(new_train_times):

            if counter==0:
                showsource['sum0']=showsource['0']
                showsource['sum19']=showsource['19']
                continue
            #showsource['mix']=showsource['mix']+showsource_list[counter].groupby('trade_date')['mix'].rank(ascending=False)

            mergedf=showsource_list[counter][['ts_code','trade_date','mix']]
            print(mergedf)
            showsource=pd.merge(showsource,mergedf,on=['ts_code','trade_date'],suffixes=('','_new'))
            
            #showsource=pd.merge(showsource, showsource_list[counter].iloc[:, ['mix']], how='left', on=['trade_date','ts_code'],suffixes=('mix','mix_2'))

            showsource['mix']=showsource['mix']+showsource['mix_new']
            showsource.drop(['mix_new'],axis=1,inplace=True)

            showsource['sum0']=showsource['sum0']+showsource_list[counter]['0']
            showsource['sum19']=showsource['sum19']+showsource_list[counter]['19']
            sfasfd=12
        

        showsource=pd.merge(showsource, all_csv_path, how='left', on=['ts_code','trade_date'])


        print(showsource['mix'])
        #databuffer=showsource['trade_date'].unique()

        #showsource2=showsource[showsource['trade_date']>20201112]
        #showsource2=showsource2[showsource2['trade_date']<20201120]
        #showsource2.to_csv('seefef2.csv')

        #剔除无法买入的涨停股
        showsource['high_stop']=0
        showsource.loc[showsource['pct_chg']>9.4,'high_stop']=1
        showsource.loc[(showsource['pct_chg']<5.2) & (4.8<showsource['pct_chg']),'high_stop']=1
        showsource.loc[showsource['pre_close']<1.5,'high_stop']=1



        #showsource.loc[showsource['total_mv_rank']>17,'high_stop']=1
        #showsource.loc[showsource['sum0']<0.1,'high_stop']=1
        #删除未大于平均值的19
        #showsource.loc[showsource['sum0']<0.06,'high_stop']=1
        showsource=showsource[showsource['high_stop']==0]



        #showsource=showsource[showsource['total_mv_rank']>17]
        #showsource=showsource[showsource['total_mv_rank']>10]

        showsource['mix_rank']=showsource.groupby('trade_date')['mix'].rank(ascending=False,pct=False,method='first')
        print(showsource)
        showsource['next_chg']=showsource.groupby('ts_code')['tomorrow_chg'].shift(-1)
        showsource['mix_rank_real']=showsource.groupby('trade_date')['tomorrow_chg'].rank(ascending=False,pct=False,method='first')

        #showsource=showsource[showsource['trade_date']>20171230]
        #showsource=showsource[showsource['ts_code'].str.startswith('300')==False]

        databuffer=showsource['trade_date'].unique()

        bufferLL=[]
        
        index=0
        maxday=len(databuffer)

        all_csv_path['buyflag']=0
        all_csv_path=all_csv_path[all_csv_path['trade_date'].isin(databuffer)]

        for curdata in databuffer:
            #curday=databuffer[index]
            cur_show=showsource[showsource["trade_date"]==curdata]
            cur_show=cur_show[cur_show['high_stop']==0]
            #cur_show=cur_show[cur_show['amount']>10000]
            #if(curdata<20180101):
            #    continue
            cc=cur_show.sort_values(by="mix" , ascending=False)

            ##简易查看
            #dd=cc['tomorrow_chg'].mean()
            #ee=cc.head(10)
            #print(curdata)
            #print(ee['ts_code'])
            #ff=ee['tomorrow_chg'].mean()
            #gg=ff-dd
            #print(gg,end='')
            #print('   ',end='')
            #print(ff,end='')
            #print('   ',end='')
            #print(curdata)

            bufferL=[]
            for i in range(dayrange):
                stringi="pct_chg_next_"+str(i+1)
                nextlist=cc.head(codechoice)[stringi].values
                bufferL.append(nextlist)

            bufferLL.append(bufferL)

            index+=1

        #print(bufferLL)
        changer=[]
        index2=0
        times=codechoice*dayrange

        for curlist in bufferLL:
            sum=0
            sumtendency=0

            for i in range(dayrange):
                buferi=index2-i
                if buferi>=0:
                    tempsum=bufferLL[buferi][i].sum()/times
                    sum+=tempsum

            changer.append(sum)

            index2+=1
        #print(changer)

        #days2,show=self.standard_show(changer,day_interval=1)
        days2,show=self.standard_show_Kelly_Criterion_new(changer,first_base_income=100000,day_interval=dayrange)
        
        self.show_screen(showsource)
        #showsource=showsource[showsource['trade_date']>20170301]
        showsource=showsource[showsource['trade_date']>20210101]
        #showsource=showsource[showsource['trade_date']<20170901]
        #showsource=showsource[showsource['mix_rank']<200]

        showsource.to_csv('seefef4.csv')
        return days2,show

    def changetoqlib(self,toqlib_df):

        #toqlib_df=pd.read_csv('./seefef2.csv',index_col=0,header=0)       
        toqlib_df=toqlib_df.loc[:,['trade_date','ts_code','mix']]

        toqlib_df['trade_date'] = pd.to_datetime(toqlib_df['trade_date'], format='%Y%m%d')

        toqlib_df['ts_codeL'] = toqlib_df['ts_code'].str[:6]
        toqlib_df['ts_codeR'] = toqlib_df['ts_code'].str[7:]

        toqlib_df['ts_code']=toqlib_df['ts_codeR'].str.cat(toqlib_df['ts_codeL'])

        toqlib_df.drop(['ts_codeL','ts_codeR'],axis=1,inplace=True)

        toqlib_df.rename(columns={'trade_date':'datetime','ts_code':'instrument', 'mix':'score'}, inplace = True)

        print(toqlib_df)

        toqlib_df.to_csv('qlibdataset.csv',index=None)

        #toqlib_df2=pd.read_csv('qlibdataset.csv',index_col=0,header=0)     

        #toqlib_df2.index = pd.to_datetime(toqlib_df2.index) 

        #print(toqlib_df2)

        intsdfafsd=6

    def show_screen(self,showsource):

        features_to_consider = ['ts_code','trade_date','total_mv_rank','pb_rank','25_pct_rank_min','25_pct_rank_max'
                                ,'sum0','sum19','pct_chg','pre_close','mix_rank']

        #features_to_consider = ['ts_code','trade_date','total_mv_rank','pb_rank'
        #                        ,'sum0','sum19','pct_chg','pre_close','mix_rank']

        #features_to_consider = ['ts_code','trade_date','pb_rank','25_pct_rank_min','25_pct_rank_max'
        #                        ,'sum0','sum19','pct_chg','pre_close','mix_rank']
        #,'total_mv_rank'
        Todayshowsource = showsource[features_to_consider]

        month_sec=Todayshowsource['trade_date'].max()
        Todayshowsource=Todayshowsource[Todayshowsource['trade_date']==month_sec]


        Todayshowsource.to_csv('Today.csv')

        return showsource

    def scatter(self,path):
        showsource=pd.read_csv(path,index_col=0,header=0)
        databuffer=showsource['trade_date'].unique()

        for curdata in databuffer:

            cur_show=showsource[showsource["trade_date"]==curdata]

            b=cur_show.sort_values(by="mix" , ascending=False) 

            x_axis=range(len(b))
            y_axis=b['tomorrow_chg']

            self.show(x_axis,y_axis,title=curdata)

            adwda=1

    def show(self,x_axis,y_axis,x_label="xlabel",y_label="ylabel ",title="title",x_tick="",y_tick="",colori="blue"):
            plt.figure(figsize=(19, 11))
            plt.scatter(x_axis, y_axis,s=8)
            #plt.xlim(30, 160)
            #plt.ylim(5, 50)
            #plt.axis()
    
            plt.title(title,color=colori)
            plt.xlabel("rank")
            plt.ylabel("chg_pct")

            if(x_tick!=""or y_tick!=""):
                plt.xticks(x_axis,x_tick)
                plt.yticks(y_axis,y_tick)

            #plt.pause(2)
            plt.show()

    def plotall(self,path):


        databuffer,show3=self.show_all_rate_ens_plus_plus_plus(path)
        #days,show3=self.show_all_rate_ens_plus_plus_random(path)   #for fhc

        #c={"trade_date":days,"pctchg":show3}
        #dfout=pd.DataFrame(c,columns=['trade_date','pctchg'])
        #dfout.to_csv('outseee0901.csv')

        days=np.arange(1,databuffer.shape[0]+1)

        eee=np.where(days%5==0)

        daysshow=days[eee]
        datashow=databuffer[eee]
        #a = np.random.rand(days.shape[0], 1)

        print(show3)

        plt.plot(days,show3,c='green',label="my model head10mean")

        plt.xticks(daysshow, datashow,color='blue',rotation=60)

        plt.legend()

        plt.show()

        input()

    def plotall_test(self):

        startdate=20130108

        ss=pd.read_csv('szzs.csv',index_col=0,header=0)


        ss=ss[ss['trade_date']>startdate]
        ss=ss.sort_values(by="trade_date" , ascending=True)
        print(ss)
        show1=ss['pct_chg'].values
        days,showfinal1=self.standard_show_para2(show1,day_interval=1)

        ss=pd.read_csv('cybz.csv',index_col=0,header=0)


        ss=ss[ss['trade_date']>startdate]
        ss=ss.sort_values(by="trade_date" , ascending=True)
        print(ss)
        show1=ss['pct_chg'].values
        _,showfinal2=self.standard_show_para2(show1,day_interval=1)

        ss=pd.read_csv('Daily2rong.csv',index_col=0,header=0)


        ss=ss.sort_values(by="trade_date" , ascending=True)
        ss=ss[ss['trade_date']>startdate]
        ss=ss[ss['exchange_id']=='SSE']

        print(ss)
        testleabel='rzrqye'
        testleabel2=testleabel+'2'

        ss[testleabel2]=ss[testleabel].shift(1)
        ss['pct_chg']=100*(ss[testleabel]-ss[testleabel2])/ss[testleabel]
        ss.fillna(0,inplace=True)
        show1=ss['pct_chg'].values
        _,showfinal3=self.standard_show_para2(show1,day_interval=1)

        #get_codeanddate_feature()

        #feature_env_codeanddate3('2017')

        #lgb_train_2('2017')

        #days,show3=self.show_all_rate_ens_plus(path)

        plt.plot(days,showfinal1,c='blue',label="000001")
        plt.plot(days,showfinal2,c='red',label="399006")
        plt.plot(days,showfinal3,c='red',label="rzrq")
        #plt.plot(days,show3,c='green',label="my model head10mean")

        plt.legend()

        plt.show()

        input()

    def real_plot_show(self):

        file_dir = "./temp3"
        a = os.walk(file_dir)
        path_list=[]
        for root, dirs, files in os.walk(file_dir):  
            path_list=files
            break
        changer=[]
        for curdata in path_list:

            cur_path="./temp3/"+curdata
            cur_df=pd.read_csv(cur_path,index_col=0,header=0)

            b=cur_df.sort_values(by="mix" , ascending=False)

            #buffer=b.head(10)
            #buffer2=buffer.sort_values(by="9" , ascending=False)
            #average=buffer2.head(1)['pct_chg'].mean()


            #average=b.tail(3)['pct_chg'].mean()
            average=b.head(10)['pct_chg'].mean()

            changer.append(average)
            adwda=1


        days2,show=self.standard_show(changer,day_interval=1)

        #plt.plot(days,show1,c='blue',label="000001")
        #plt.plot(days,show2,c='red',label="399006")
        plt.plot(days2,show,c='green',label="my model real")

        plt.legend()

        plt.show()

    def parafirst(self,path):
        new_train_times=4
        showsource_list=[]
        for counter in range(new_train_times):
            path_new=os.path.splitext(path)[0]+str(counter)+".csv"

            ss=pd.read_csv(path_new,index_col=0,header=0)           
            
            showsource_list.append(ss)

        return showsource_list

    def paramain(self,showsource_get,Y):
            
        showsource_list=[]
        new_train_times=4
        for counter in range(new_train_times):
            ss=showsource_get[counter]
            ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]
            #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]+ss['10']*Y[10]+ss['11']*Y[11]+ss['12']*Y[12]+ss['13']*Y[13]+ss['14']*Y[14]
            #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]
            showsource_list.append(ss)
            

        #showsource_list[0]['mix']=showsource_list[0].groupby('trade_date')['mix'].rank(ascending=False)
        showsource=showsource_list[0]
        for counter in range(new_train_times):
            if counter==0:
                continue
            #showsource['mix']=showsource['mix']+showsource_list[counter].groupby('trade_date')['mix'].rank(ascending=False)
            showsource['mix']=showsource['mix']+showsource_list[counter]['mix']
            sfasfd=12
            
        #print(showsource['mix'])
        databuffer=showsource['trade_date'].unique()


        #showsource['mix_rank']=showsource.groupby('trade_date')['mix'].rank(ascending=False)
        #showsource['next_chg']=showsource.groupby('ts_code')['tomorrow_chg'].shift(-1)
        changer=[]
        for curdata in databuffer:

            cur_show=showsource[showsource["trade_date"]==curdata]
            b=cur_show.sort_values(by="mix" , ascending=False)
            #b=cur_show.sort_values(by="9" , ascending=True)
            #d=b.head(10)
            #e=d.sort_values(by="mix" , ascending=True)
            

            #buffer=b.head(10)
            #buffer2=buffer.sort_values(by="9" , ascending=False)
            #average=buffer2.head(1)['tomorrow_chg'].mean()

            #b=cur_show[cur_show['mix']>0.40]
            average=b.head(50)['tomorrow_chg'].mean()
            #average=b.tail(10)['tomorrow_chg'].mean()
            changer.append(average)

            adwda=1


        days2,show=self.standard_show_para(changer,day_interval=5)
        #days2,show=self.standard_show_Kelly_Criterion(changer,day_interval=1)
        
        #showsource=showsource[showsource['mix_rank']<10]
        #showsource.to_csv('seefef.csv')
        return show[-1]

    def real_plot_show_plus(self):

        new_train_times=4

        Y=[-12,-8,-3,-2,-1,1,2,3,8,18]

        #Y=[-12,-6,-3,-2,-1,1,2,3,6,12]

        all_csv_path=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
        all_csv_path=all_csv_path.loc[:,['ts_code','trade_date','pct_chg']]
        all_csv_path=all_csv_path[-1000000:]
        all_csv_path['pct_chg']=all_csv_path['pct_chg'].astype('float64')

        #all_csv_path['pct_chg']=all_csv_path.groupby('ts_code')['pct_chg'].shift(-1)

        #明日幅度
        tm1=all_csv_path.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=all_csv_path.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=all_csv_path.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=all_csv_path.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=all_csv_path.groupby('ts_code')['pct_chg'].shift(-5)
        #tm6=all_csv_path.groupby('ts_code')['pct_chg'].shift(-6)
        #tm7=all_csv_path.groupby('ts_code')['pct_chg'].shift(-7)
        ##df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        #tm8=all_csv_path.groupby('ts_code')['pct_chg'].shift(-8)
        #tm9=all_csv_path.groupby('ts_code')['pct_chg'].shift(-9)
        #tm10=all_csv_path.groupby('ts_code')['pct_chg'].shift(-10)

        #all_csv_path['pct_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)*
        #                         ((100+tm6)/100)*((100+tm7)/100)*((100+tm8)/100)*((100+tm9)/100)*((100+tm10)/100)-1)*100
        all_csv_path['pct_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100
        #all_csv_path['pct_chg']=(((100+tm1)/100)-1)*100

        file_dir = "./temp3"
        a = os.walk(file_dir)
        path_list=[]
        for root, dirs, files in os.walk(file_dir):  
            path_list=files
            break

        firstdata="./temp3/"+path_list[0]

        all_real_data=pd.read_csv(firstdata,index_col=0,header=0)
        print(all_real_data)

        for curdata in path_list:
            if(curdata==path_list[0]):
                continue

            cur_path="./temp3/"+curdata
            cur_df=pd.read_csv(cur_path,index_col=0,header=0)

            all_real_data=pd.concat([all_real_data,cur_df],axis=0)

            adwda=1

        all_real_data=all_real_data.rename(columns = {"trade_date_x": "trade_date"})
        #all_real_data['trade_date']=all_real_data['trade_date'].astype('object')

        #all_csv_path['ts_code']=all_csv_path['ts_code'].astype('int64')

        all_csv_path['ts_code']=all_csv_path['ts_code'].map(lambda x : x[:-3])
        all_csv_path['ts_code']=all_csv_path['ts_code'].astype('int64')

        showsource=pd.merge(all_real_data, all_csv_path, how='left', on=['ts_code','trade_date'])

        databuffer=showsource['trade_date'].unique()

        showsource['mix_rank']=showsource.groupby('trade_date')['mix'].rank(ascending=False,pct=False,method='first')
        #print(showsource)
        #showsource['next_chg']=showsource.groupby('ts_code')['tomorrow_chg'].shift(-1)
        changer=[]
        for curdata in databuffer:

            cur_show=showsource[showsource["trade_date"]==curdata]
            b=cur_show.sort_values(by="mix" , ascending=False)
            #b=cur_show.sort_values(by="9" , ascending=True)
            #d=b.head(10)
            #e=d.sort_values(by="mix" , ascending=True)
            

            #buffer=b.tail(20)
            #buffer2=buffer.sort_values(by="0" , ascending=False)
            #average=buffer2.tail(1)['tomorrow_chg'].mean()

            #b=cur_show[cur_show['mix']>0.40]
            average=b.head(30)['pct_chg_y'].mean()
            #average=b.tail(2000)['pct_chg_y'].mean()
            #average=b.head(3)['tomorrow_chg'].mean()
            #average=b.tail(15)['tomorrow_chg'].mean()
            #if(average>10):
            #    sdsdf=1
                
            changer.append(average)

            adwda=1
        print(changer)

        days2,show=self.standard_show(changer,day_interval=5)

        #showsource=showsource[showsource['mix_rank']<10]
        showsource.to_csv('seerealfef.csv')

        #plt.plot(days,show1,c='blue',label="000001")
        #plt.plot(days,show2,c='red',label="399006")
        plt.plot(days2,show,c='green',label="my model real")

        plt.legend()

        plt.show()

        zfseseg=1

    def real_plot_create(self):

        file_dir = "./temp2"
        a = os.walk(file_dir)
        path_list=[]
        for root, dirs, files in os.walk(file_dir):  
            path_list=dirs
            break

        cur=path_list[0][6:]
        for curpath in path_list:
            next=curpath[6:]
            if(next!=cur):
                self._remix_csv(cur,next)

            cur=next
            sdsd=1
        asdad=1

    def _remix_csv(self,predit_date,next_date):

        result_path='./temp3/'+next_date+'.csv'
        isExists=os.path.exists(result_path)
        # 判断结果
        if isExists:
            print(result_path+' 文件已存在')
            return

        remixpath='./temp2/result'+predit_date

        show=pd.read_csv(remixpath+'/out1.csv',index_col=0,header=0)
        show2=pd.read_csv(remixpath+'/out2.csv',index_col=0,header=0)
        show3=pd.read_csv(remixpath+'/out3.csv',index_col=0,header=0)
        show4=pd.read_csv(remixpath+'/out4.csv',index_col=0,header=0)

        #show['mix']=show['mix'].rank(ascending=True)
        #show2['mix']=show2['mix'].rank(ascending=True)
        #show3['mix']=show3['mix'].rank(ascending=True)
        #show4['mix']=show4['mix'].rank(ascending=True)

        show['9']=show['9']+show2['9']+show3['9']+show4['9']
        show['mix']=show['mix']+show2['mix']+show3['mix']+show4['mix']
        
        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()
        pro = ts.pro_api(token)

        df = pro.daily(trade_date=next_date)
        print(df)
        df['ts_code']=df['ts_code'].map(lambda x : x[:-3])

        df['ts_code'] = df['ts_code'].astype(int64)
        print(df)
        result = pd.merge(show, df, how='left', on=['ts_code'])


        result.to_csv(result_path)

        fsfef=1

    def show_all_rate_ens_plus_plus_random(self,path):

        new_train_times=4

        Y=[-12,-6,-3,-2,-1,0,0,0,0,0,0,1,1,1,1,1,2,3,7,14]
        #Y=[-48,-6,-3,-2,-1,0,0,0,0,0,0,1,1,1,1,1,2,3,6,48]
        #Y=[-12,-6,-3,-2,-1,1,2,3,6,12]
        #Y=[0,0,0,0,0,0,0,0,0,100]
        #Y=[-8,-4,-3,-2,-1,1,2,3,4,8]
        #Y=[-12,0,0,0,0,0,0,0,0,12]
        #Y=[-12,-7,-3,-2,-1,1,2,5,10,18]
        #Y=[-3,-2,-1,-0.5,0,0,0.5,1,2,3]
        #Y=[3,2,1,0.5,0,0,-0.5,-1,-2,-3]
        #Y=[-12,-8,-6,-4,-3,0,0,0,0,0]
        #Y=[-12,-8,-3,-2,-1,1,2,3,10,18]
        #Y=[-12,0,0,0,0,0,0,0,8,18]
        #Y2=[-12,-8,-3,-2,-1,1,2,3,6,12]
        #Y=[-12,-7,-3,-2,-1,1,0,1,4,7]

        codechoice=30
        dayrange=5

        all_csv_path=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)       
        all_csv_path=all_csv_path.loc[:,['ts_code','trade_date','pct_chg','pre_close','open','amount']]
        all_csv_path['pct_chg']=all_csv_path['pct_chg'].astype('float64')

        all_csv_path_fhc=pd.read_csv('./test04_total.csv',index_col=0,header=0)   

        all_csv_path['open']=all_csv_path['open'].astype('float64')
        all_csv_path['next_open']=all_csv_path.groupby('ts_code')['open'].shift(0)
        all_csv_path['next_open2']=all_csv_path.groupby('ts_code')['open'].shift(-1)
        all_csv_path['pct_chg']=((all_csv_path['next_open2']-all_csv_path['next_open'])/(all_csv_path['next_open']+0.00001))*100
        #all_csv_path['open']=all_csv_path['pre_close'].astype('float64')
        #all_csv_path['next_open']=all_csv_path.groupby('ts_code')['open'].shift(0)
        #all_csv_path['next_open2']=all_csv_path.groupby('ts_code')['open'].shift(-1)
        #all_csv_path['pct_chg']=((all_csv_path['next_open2']-all_csv_path['next_open'])/(all_csv_path['next_open']+0.00001))*100


        #all_csv_path['pct_chg']=all_csv_path.groupby('ts_code')['pct_chg'].shift(-1)

        #提取几天的pctchg
        for i in range(dayrange):
            shifti=-i-1
            tm=all_csv_path.groupby('ts_code')['pct_chg'].shift(shifti)
            tm.fillna(0, inplace=True)
            stringi="pct_chg_next_"+str(i+1)
            all_csv_path[stringi]=tm

        showsource_list=[]
        for counter in range(new_train_times):
            path_new=os.path.splitext(path)[0]+str(counter)+".csv"

            ss=pd.read_csv(path_new,index_col=0,header=0)
            if(counter<4):
                #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]+ss['10']*Y[10]+ss['11']*Y[11]+ss['12']*Y[12]+ss['13']*Y[13]+ss['14']*Y[14]
                ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]+ss['10']*Y[10]+ss['11']*Y[11]+ss['12']*Y[12]+ss['13']*Y[13]+ss['14']*Y[14]+ss['15']*Y[15]+ss['16']*Y[16]+ss['17']*Y[17]+ss['18']*Y[18]+ss['19']*Y[19]

                #ss['mix']=(ss['0']**0.5)*Y[0]+(ss['1']**0.5)*Y[1]+(ss['2']**0.5)*Y[2]+(ss['3']**0.5)*Y[3]+(ss['4']**0.5)*Y[4]+(ss['5']**0.5)*Y[5]+(ss['6']**0.5)*Y[6]+(ss['7']**0.5)*Y[7]+(ss['8']**0.5)*Y[8]+(ss['9']**0.5)*Y[9]
                #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]
                #ss['mix']=(ss['0']**2)*Y[0]+(ss['1']**2)*Y[1]+(ss['2']**2)*Y[2]+(ss['3']**2)*Y[3]+(ss['4']**2)*Y[4]+(ss['5']**2)*Y[5]+(ss['6']**2)*Y[6]+(ss['7']**2)*Y[7]+(ss['8']**2)*Y[8]+(ss['9']**2)*Y[9]
                #ss['mix']=(ss['9']/ss['0'])*8+(ss['8']/ss['1'])*4+(ss['7']/ss['2'])*2+(ss['6']/ss['3'])*1
                ss['mix']=ss.groupby('trade_date')['mix'].rank(ascending=True,pct=False,method='first')
            else:
                ss['mix']=ss['0']
                ss['mix']=ss.groupby('trade_date')['mix'].rank(ascending=True,pct=False,method='first')

            #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]
            showsource_list.append(ss)
        
        #showsource_list[0]['mix']=showsource_list[0].groupby('trade_date')['mix'].rank(ascending=False)
        showsource=showsource_list[0]
        print(showsource_list[0])
        for counter in range(new_train_times):
            if counter==0:
                #showsource['sum0']=showsource['0']
                continue
            #showsource['mix']=showsource['mix']+showsource_list[counter].groupby('trade_date')['mix'].rank(ascending=False)
            showsource['mix']=showsource['mix']+showsource_list[counter]['mix']
            #showsource['sum0']=showsource['sum0']+showsource_list[counter]['0']
            sfasfd=12
        
        showsource=pd.merge(showsource, all_csv_path, how='left', on=['ts_code','trade_date'])

        showsource=pd.merge(showsource, all_csv_path_fhc, how='left', on=['ts_code','trade_date'])

        showsource.dropna(axis=0,how='any',inplace=True)

        print(showsource['mix'])
        databuffer=showsource['trade_date'].unique()

        showsource.to_csv('seefef2.csv')
        showsource['mix']=showsource['pred']


        #剔除无法买入的涨停股
        showsource['high_stop']=0
        showsource.loc[showsource['pct_chg']>9.4,'high_stop']=1
        showsource.loc[(showsource['pct_chg']<5.2) & (4.8<showsource['pct_chg']),'high_stop']=1
        showsource.loc[showsource['pre_close']<1.5,'high_stop']=1
        #showsource.loc[showsource['total_mv_rank']>17,'high_stop']=1
        #showsource.loc[showsource['0']>0.01,'high_stop']=1
        #删除未大于平均值的19
        #showsource.loc[showsource['sum0']<0.06,'high_stop']=1
        showsource=showsource[showsource['high_stop']==0]



        #showsource=showsource[showsource['total_mv_rank']>17]
        #showsource=showsource[showsource['total_mv_rank']>10]

        showsource['mix_rank']=showsource.groupby('trade_date')['mix'].rank(ascending=False,pct=False,method='first')
        print(showsource)
        showsource['next_chg']=showsource.groupby('ts_code')['tomorrow_chg'].shift(-1)
        showsource['mix_rank_real']=showsource.groupby('trade_date')['tomorrow_chg'].rank(ascending=False,pct=False,method='first')


        #showsource=showsource[showsource['ts_code'].str.startswith('300')==False]

        bufferLL=[]
        
        index=0
        maxday=len(databuffer)

        all_csv_path['buyflag']=0
        all_csv_path=all_csv_path[all_csv_path['trade_date'].isin(databuffer)]

        for curdata in databuffer:
            #curday=databuffer[index]
            cur_show=showsource[showsource["trade_date"]==curdata]
            cur_show=cur_show[cur_show['high_stop']==0]
            #cur_show=cur_show[cur_show['amount']>10000]
            #if(curdata<20180101):
            #    continue
            cc=cur_show.sort_values(by="mix" , ascending=False)

            ##简易查看
            #dd=cc['tomorrow_chg'].mean()
            ee=cc.tail(5)
            print(ee['ts_code'])
            #ff=ee['tomorrow_chg'].mean()
            #gg=ff-dd
            #print(gg,end='')
            #print('   ',end='')
            #print(ff,end='')
            #print('   ',end='')
            #print(curdata)

            bufferL=[]
            for i in range(dayrange):
                stringi="pct_chg_next_"+str(i+1)
                nextlist=cc.tail(codechoice)[stringi].values
                bufferL.append(nextlist)

            print(curdata)
            print(bufferL)
            bufferLL.append(bufferL)

            index+=1

        #print(bufferLL)
        changer=[]
        index2=0
        times=codechoice*dayrange

        tendencyrange=30
        lastsumlist = np.zeros(tendencyrange)
        newtendlist=np.ones(dayrange)

        for curlist in bufferLL:
            sum=0
            sumtendency=0

            for i in range(dayrange):
                buferi=index2-i
                if buferi>=0:
                    tempsum=bufferLL[buferi][i].sum()/times
                    sum+=tempsum
                    sumtendency+=(newtendlist[i]*tempsum)

            for i in range(dayrange):
                if((dayrange-i)>1):
                    newtendlist[dayrange-i-1]=newtendlist[dayrange-i-2]
                else:
                    break

            if(lastsumlist.sum()>0):
                sum2=sum
                newtendlist[0]=1
            else:
                sum2=0
                newtendlist[0]=0

            #changer.append(sumtendency)
            changer.append(sum)
            print(sumtendency,end='')
            print('   ',end='')
            print(index2)

            for i in range(tendencyrange):
                if((tendencyrange-i)>1):
                    lastsumlist[tendencyrange-i-1]=lastsumlist[tendencyrange-i-2]
                else:
                    break
            lastsumlist[0]=sum


            index2+=1
        #print(changer)

        #days2,show=self.standard_show(changer,day_interval=1)
        days2,show=self.standard_show_Kelly_Criterion_new(changer,first_base_income=100000,day_interval=dayrange)
        
        #showsource=showsource[showsource['trade_date']>20190401]
        showsource=showsource[showsource['trade_date']>20210101]
        #showsource=showsource[showsource['mix_rank']<200]
        showsource.to_csv('seefeffhc4.csv')
        return days2,show

    def show_all_rate_ens_plus_plus_reg(self,path):

        new_train_times=4

        #Y=[-12,-6,-3,-2,-1,1,2,3,6,12]
        #Y=[-12,-6,-3,-2,-1,1,2,3,6,12]
        #Y=[-12,-8,-6,-4,-3,0,0,0,0,0]
        #Y=[-12,-8,-3,-2,-1,1,2,3,10,18]
        #Y=[-12,0,0,0,0,0,0,0,8,18]
        #Y2=[-12,-8,-3,-2,-1,1,2,3,6,12]
        #Y=[-12,-7,-3,-2,-1,1,0,1,4,7]

        codechoice=50
        dayrange=5

        all_csv_path=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)       
        all_csv_path=all_csv_path.loc[:,['ts_code','trade_date','pct_chg','pre_close','amount']]
        all_csv_path['pct_chg']=all_csv_path['pct_chg'].astype('float64')

        #all_csv_path['pct_chg']=all_csv_path.groupby('ts_code')['pct_chg'].shift(-1)

        #提取几天的pctchg
        for i in range(dayrange):
            shifti=-i-1
            tm=all_csv_path.groupby('ts_code')['pct_chg'].shift(shifti)
            tm.fillna(0, inplace=True)
            stringi="pct_chg_next_"+str(i+1)
            all_csv_path[stringi]=tm

        showsource_list=[]
        for counter in range(new_train_times):
            path_new=os.path.splitext(path)[0]+str(counter)+".csv"

            ss=pd.read_csv(path_new,index_col=0,header=0)
            #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]+ss['10']*Y[10]+ss['11']*Y[11]+ss['12']*Y[12]+ss['13']*Y[13]+ss['14']*Y[14]
            ss['mix']=ss['0']
            #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]
            showsource_list.append(ss)
        
        #showsource_list[0]['mix']=showsource_list[0].groupby('trade_date')['mix'].rank(ascending=False)
        showsource=showsource_list[0]
        print(showsource_list[0])
        for counter in range(new_train_times):
            if counter==0:
                continue
            #showsource['mix']=showsource['mix']+showsource_list[counter].groupby('trade_date')['mix'].rank(ascending=False)
            showsource['mix']=showsource['mix']+showsource_list[counter]['mix']
            sfasfd=12
        
        showsource=pd.merge(showsource, all_csv_path, how='left', on=['ts_code','trade_date'])

        print(showsource['mix'])
        databuffer=showsource['trade_date'].unique()

        #剔除无法买入的涨停股
        showsource['high_stop']=0
        showsource.loc[showsource['pct_chg']>9.4,'high_stop']=1
        showsource.loc[(showsource['pct_chg']<5.2) & (4.8<showsource['pct_chg']),'high_stop']=1
        showsource.loc[showsource['pre_close']<1.5,'high_stop']=1        
        #showsource=showsource[showsource['high_stop']==0]

        #showsource=showsource[showsource['amount']>10000]

        showsource['mix_rank']=showsource.groupby('trade_date')['mix'].rank(ascending=False,pct=False,method='first')
        print(showsource)
        showsource['next_chg']=showsource.groupby('ts_code')['tomorrow_chg'].shift(-1)
        showsource['mix_rank_real']=showsource.groupby('trade_date')['tomorrow_chg'].rank(ascending=False,pct=False,method='first')


        #showsource=showsource[showsource['ts_code'].str.startswith('300')==False]

        bufferLL=[]
        
        index=0
        maxday=len(databuffer)

        all_csv_path['buyflag']=0
        all_csv_path=all_csv_path[all_csv_path['trade_date'].isin(databuffer)]

        for curdata in databuffer:
            #curday=databuffer[index]
            cur_show=showsource[showsource["trade_date"]==curdata]
            cur_show=cur_show[cur_show['high_stop']==0]
            #cur_show=cur_show[cur_show['amount']>10000]
            #if(curdata<20180101):
            #    continue
            cc=cur_show.sort_values(by="mix" , ascending=False)

            bufferL=[]
            for i in range(dayrange):
                stringi="pct_chg_next_"+str(i+1)
                nextlist=cc.head(codechoice)[stringi].values
                bufferL.append(nextlist)

            bufferLL.append(bufferL)

            index+=1

        #print(bufferLL)
        changer=[]
        index2=0
        times=codechoice*dayrange
        for curlist in bufferLL:
            sum=0

            for i in range(dayrange):
                buferi=index2-i
                if buferi>=0:
                    sum+=bufferLL[buferi][i].sum()/times

            changer.append(sum)

            index2+=1
        #print(changer)

        #days2,show=self.standard_show(changer,day_interval=1)
        days2,show=self.standard_show_Kelly_Criterion_new(changer,first_base_income=100000,day_interval=dayrange)
        
        #showsource=showsource[showsource['trade_date']>20190101]
        showsource=showsource[showsource['mix_rank']<20]
        showsource.to_csv('seefef.csv')
        return days2,show

    def show_all_rate_ens_plus_notrank(self,path):

        new_train_times=4

        #Y=[-12,-6,-3,-2,-1,1,2,3,6,12]
        #Y=[-12,-6,-3,-2,-1,1,2,3,8,18]
        Y=[-12,0,0,0,0,0,0,0,8,18]
        #Y2=[-12,-8,-3,-2,-1,1,2,3,6,12]
        #Y=[-12,-7,-3,-2,-1,1,0,1,4,7]

        codechoice=3
        dayrange=5

        all_csv_path=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
        all_csv_path=all_csv_path.loc[:,['ts_code','trade_date','pct_chg','pre_close']]
        all_csv_path['pct_chg']=all_csv_path['pct_chg'].astype('float64')

        #all_csv_path['pct_chg']=all_csv_path.groupby('ts_code')['pct_chg'].shift(-1)

        #明日幅度
        tm1=all_csv_path.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=all_csv_path.groupby('ts_code')['pct_chg'].shift(-2)
        tm3=all_csv_path.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=all_csv_path.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=all_csv_path.groupby('ts_code')['pct_chg'].shift(-5)
        tm6=all_csv_path.groupby('ts_code')['pct_chg'].shift(-6)
        tm7=all_csv_path.groupby('ts_code')['pct_chg'].shift(-7)
        tm8=all_csv_path.groupby('ts_code')['pct_chg'].shift(-8)
        tm9=all_csv_path.groupby('ts_code')['pct_chg'].shift(-9)
        tm10=all_csv_path.groupby('ts_code')['pct_chg'].shift(-10)

        all_csv_path['pct_chg_next_1']=tm1
        all_csv_path['pct_chg_next_2']=tm2
        all_csv_path['pct_chg_next_3']=tm3
        all_csv_path['pct_chg_next_4']=tm4
        all_csv_path['pct_chg_next_5']=tm5

        showsource_list=[]
        for counter in range(new_train_times):
            path_new=os.path.splitext(path)[0]+str(counter)+".csv"

            ss=pd.read_csv(path_new,index_col=0,header=0)
            #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]+ss['10']*Y[10]+ss['11']*Y[11]+ss['12']*Y[12]+ss['13']*Y[13]+ss['14']*Y[14]
            ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]
            #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]
            showsource_list.append(ss)
        
        #showsource_list[0]['mix']=showsource_list[0].groupby('trade_date')['mix'].rank(ascending=False)
        showsource=showsource_list[0]
        print(showsource_list[0])
        for counter in range(new_train_times):
            if counter==0:
                continue
            #showsource['mix']=showsource['mix']+showsource_list[counter].groupby('trade_date')['mix'].rank(ascending=False)
            showsource['mix']=showsource['mix']+showsource_list[counter]['mix']
            sfasfd=12
        
        showsource=pd.merge(showsource, all_csv_path, how='left', on=['ts_code','trade_date'])

        print(showsource['mix'])
        databuffer=showsource['trade_date'].unique()

        #剔除无法买入的涨停股
        showsource['high_stop']=0
        showsource.loc[showsource['pct_chg']>9.4,'high_stop']=1
        showsource.loc[(showsource['pct_chg']<5.2) & (4.8<showsource['pct_chg']),'high_stop']=1
        showsource.loc[showsource['pre_close']<1.5,'high_stop']=1        
        showsource=showsource[showsource['high_stop']==0]

        showsource['mix_rank']=showsource.groupby('trade_date')['mix'].rank(ascending=False,pct=False,method='first')
        print(showsource)
        showsource['next_chg']=showsource.groupby('ts_code')['tomorrow_chg'].shift(-1)



        bufferLL=[]
        

        index=0
        maxday=len(databuffer)

        all_csv_path['buyflag']=0
        all_csv_path=all_csv_path[all_csv_path['trade_date'].isin(databuffer)]

        for curdata in databuffer:
            #curday=databuffer[index]
            cur_show=showsource[showsource["trade_date"]==curdata]
            cc=cur_show.sort_values(by="mix" , ascending=False)

            next1list=cc.head(codechoice)['pct_chg_next_1'].values
            next2list=cc.head(codechoice)['pct_chg_next_2'].values
            next3list=cc.head(codechoice)['pct_chg_next_3'].values
            next4list=cc.head(codechoice)['pct_chg_next_4'].values
            next5list=cc.head(codechoice)['pct_chg_next_5'].values

            bufferL=[]
            bufferL.append(next1list)
            bufferL.append(next2list)
            bufferL.append(next3list)
            bufferL.append(next4list)
            bufferL.append(next5list)
            bufferLL.append(bufferL)

            #index_inner=0
            #for i in range(5):
            #    curindex=index+index_inner
            #    if maxday <= curindex:
            #        break

            #    curuseday=databuffer[curindex]
            #    #此方法太慢了
            #    #all_csv_path.loc[all_csv_path['ts_code'].isin(choicedlist) & (all_csv_path['trade_date']==curuseday),'buyflag']=1
            #    bufferLL.append()
            #    index_inner+=1

            index+=1

        #print(bufferLL)
        changer=[]
        index2=0
        times=codechoice*dayrange
        for curlist in bufferLL:
            sum=0
            bufer2=index2-1
            bufer3=index2-2
            bufer4=index2-3
            bufer5=index2-4
            sum=bufferLL[index2][0].sum()/times
            if bufer2>=0:
                sum+=bufferLL[bufer2][1].sum()/times
            if bufer3>=0:
                sum+=bufferLL[bufer3][2].sum()/times
            if bufer4>=0:
                sum+=bufferLL[bufer4][3].sum()/times
            if bufer5>=0:
                sum+=bufferLL[bufer5][4].sum()/times

            #print(curlist)

            changer.append(sum)

            index2+=1
        print(changer)

        #days2,show=self.standard_show(changer,day_interval=1)
        days2,show=self.standard_show_Kelly_Criterion_new(changer,first_base_income=100000,day_interval=dayrange)
        
        #showsource=showsource[showsource['trade_date']>20190101]
        showsource=showsource[showsource['mix_rank']<10]
        showsource.to_csv('seefef.csv')
        return days2,show

    def standard_show(self,changer,first_base_income=100000,day_interval=2,label="自己"):
    
        start_from=first_base_income
        show=[]
        for curchange in changer:
            start_from=start_from+(first_base_income/100/day_interval)*curchange-0.0020*first_base_income/day_interval
            show.append(start_from)

        #print(show)
        len_show=len(show)
        days=np.arange(1,len_show+1)

        fig=plt.figure(figsize=(6,3))


        #plt.show()

        return days,show

    def standard_show_para(self,changer,first_base_income=100000,day_interval=2):
    
        start_from=first_base_income
        show=[]
        for curchange in changer:
            start_from=start_from+(first_base_income/100/day_interval)*curchange-0.0020*first_base_income/day_interval
            show.append(start_from)

        #print(show)
        len_show=len(show)
        days=np.arange(1,len_show+1)

        return days,show

    def standard_show_para2(self,changer,first_base_income=100000,day_interval=2):
    
        start_from=first_base_income
        show=[]
        for curchange in changer:
            start_from=start_from+(first_base_income/100/day_interval)*curchange
            show.append(start_from)

        #print(show)
        len_show=len(show)
        days=np.arange(1,len_show+1)

        return days,show

    def standard_show_Kelly_Criterion_new(self,changer,first_base_income=1000000,day_interval=2,label="自己"):
        Kelly_rate=0.90
        start_from=first_base_income
        show=[]
        for curchange in changer:

            #start_from=start_from*(1+curchange/100-0.0015/day_interval)*Kelly_rate+start_from*(1-Kelly_rate)
            start_from=start_from*((1+curchange/100)*(1-0.0015/day_interval)*Kelly_rate+(1-Kelly_rate))
            #start_from=start_from*(1+curchange/100-0.0020/5)*Kelly_rate+start_from*(1-Kelly_rate)
            #print(curchange)
            #print(start_from)
            show.append(start_from)

        #print(show)
        len_show=len(show)
        days=np.arange(1,len_show+1)

        #fig=plt.figure(figsize=(6,3))


        #plt.show()

        return days,show

    def show_today(self):

        show=pd.read_csv('out1.csv',index_col=0,header=0)
        show2=pd.read_csv('out2.csv',index_col=0,header=0)
        show3=pd.read_csv('out3.csv',index_col=0,header=0)
        show4=pd.read_csv('out4.csv',index_col=0,header=0)

        show['9']=show['9']+show2['9']+show3['9']+show4['9']
        show['mix']=show['mix']+show2['mix']+show3['mix']+show4['mix']


        datamax=show['trade_date'].max()
        #datamax=20190408

        show=show[show['trade_date']==datamax]

        show=show[['ts_code','0','9','mix']]

        #ascending表示升降序
        b=show.sort_values(by="mix" , ascending=False) 
        c=show.sort_values(by="9" , ascending=False) 
        final_mix=b.head(10)
        final_9=c.head(10)

        arr=['600461','603389','300384']
        final_have=show[show['ts_code'].isin(arr)]

        pd.set_option('display.max_columns', None)
        print('综合成绩')
        print(final_mix)
        print('极限成绩')
        print(final_9)
        print('当前拥有')
        print(final_have)

        b.to_csv("today_real_remix_result.csv")

        fsfef=1


