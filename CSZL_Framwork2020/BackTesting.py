#coding=utf-8

import Display
import FeatureEnvironment as FE
import Dataget
import Models

import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt

class BackTesting(object):
    """description of class"""

    def TodayTesting(self):

        SuperGet=Dataget.Dataget()

        updateday="20211224"

        ####刷新资金量
        SuperGet.update_moneyflow('20130101',updateday)

        ###刷新数据库
        SuperGet.updatedaily('20100101',updateday)

        ###刷新复权因子
        SuperGet.updatedaily_adj_factor('20100101',updateday)

        ###刷新经济指标
        SuperGet.updatedaily_long_factors('20100101',updateday)

        ###刷新个股波动范围
        SuperGet.update_stk_limit('20100101',updateday)

        ######刷新个股板块信息
        ####SuperGet.update_concept()

        ####SuperGet.update_basic()


        #dayA='20130620'
        #dayB='20160603'
        #dayC='20160420'
        #dayD='20200919'
        
        #dayA='20170220'
        dayA='20130219'
        dayB='20211120'
        dayC='20210501'
        dayD='20211224'

        #dayA='20170219'
        #dayB='20190528'
        #dayC='20190601'
        #dayD='20210604'

        #dayA='20190119'
        #dayB='20190528'
        #dayC='20190601'
        #dayD='20200604'
        
        #dayA='20190401'
        #dayB='20210913'
        #dayC='20130219'
        #dayD='20190401'

        #dayA='20130219'
        #dayB='20160901'
        #dayC='20160901'
        #dayD='20200904'
        #dayC='20210101'
        #dayD='20210604'

        #dayA='20130219'
        #dayB='20170528'
        #dayC='20170601'
        #dayD='20210604'

        #dayA='20130219'
        #dayB='20190528'
        #dayC='20190601'
        #dayD='20210820'

        #dayC='20210201'
        #dayD='20211029'

        #Default_folder_path='./temp/'
        Default_folder_path='D:/temp/'

        ##选择日期
        dataset_adj_train=SuperGet.getDataSet_adj_factor(dayA,dayB,Default_folder_path)
        dataset_adj_test=SuperGet.getDataSet_adj_factor(dayC,dayD,Default_folder_path)

        dataset_train=SuperGet.getDataSet(dayA,dayB,Default_folder_path)
        dataset_test=SuperGet.getDataSet(dayC,dayD,Default_folder_path)

        #测试添加长期指标
        dataset_long_train=SuperGet.getDataSet_long_factor(dayA,dayB,Default_folder_path)
        dataset_long_test=SuperGet.getDataSet_long_factor(dayC,dayD,Default_folder_path)

        ##添加确定的stflag防止模型与实际情况的区别
        dataset_stk_limit_train=SuperGet.getDataSet_stk_limit(dayA,dayB,Default_folder_path)
        dataset_stk_limit_test=SuperGet.getDataSet_stk_limit(dayC,dayD,Default_folder_path)

        ###测试添加资金量指标
        dataset_moneyflow_train=SuperGet.getDataSet_moneyflow(dayA,dayB,Default_folder_path)
        dataset_moneyflow_test=SuperGet.getDataSet_moneyflow(dayC,dayD,Default_folder_path)
        #dataset_moneyflow_train=[]
        #dataset_moneyflow_test=[]

        #加上基础板块指标等固定属性
        dataset_basic,dataset_conceptlist=SuperGet.getDataSet_basic()

        #加上板块固定属性
        dataset_concept=SuperGet.getDataSet_concept()
        #mix概念信息为类似onehot
        dataset_conceptmixed=SuperGet.getDataSet_conceptmixed()

        #选择特征工程
        
        #cur_fe=FE.FEg30eom0110network()
        #cur_fe=FE.FEg30eom0110onlinew6()
        
        cur_fe=FE.FE_a31()
        #cur_fe=FE.FE_qliba2()
        
        #cur_fe=FE.FEfast_b02()
        
        #cur_fe=FE.FEonlinew_a21()
        #cur_fe=FE.FEfast_a23_pos()
        
        
        #cur_fe=FE.trend_following()
        #cur_fe=FE.FEg30eom_start1213a()

        FE_train=cur_fe.create(dataset_train,dataset_adj_train,dataset_stk_limit_train,\
            dataset_moneyflow_train,dataset_long_train,dataset_basic,dataset_conceptlist,\
            dataset_concept,dataset_conceptmixed)
        FE_test=cur_fe.create(dataset_test,dataset_adj_test,dataset_stk_limit_test,\
            dataset_moneyflow_test,dataset_long_test,dataset_basic,dataset_conceptlist,\
            dataset_concept,dataset_conceptmixed)

        #FE_train=cur_fe.create(True,dataset_train,dataset_adj_train,dataset_stk_limit_train,\
        #    dataset_moneyflow_train,dataset_long_train,dataset_basic,dataset_conceptlist,\
        #    dataset_concept,dataset_conceptmixed)
        #FE_test=cur_fe.create(False,dataset_test,dataset_adj_test,dataset_stk_limit_test,\
        #    dataset_moneyflow_test,dataset_long_test,dataset_basic,dataset_conceptlist,\
        #    dataset_concept,dataset_conceptmixed)

        #选择模型
        #cur_model=Models.Networkmodel_20()
        cur_model=Models.LGBmodel_20()
        
        #cur_model=Models.LGBmodel_reg()

        #训练模型
        cur_model_done=cur_model.train(FE_train)
        #进行回测
        finalpath=cur_model.predict(FE_test,cur_model_done)
    
        #展示类
        dis=Display.Display()

        #dis.scatter(finalpath)
        dis.plotall(finalpath)


        sdfsdf=1

    def changetoqlib2(self):

        toqlib_df=pd.read_csv('./seesee.csv',index_col=0,header=0)       
        toqlib_df2=pd.read_csv('./qlibdataset.csv',index_col=0,header=0)   

        toqlib_df=pd.merge(toqlib_df, toqlib_df2, how='left', on=['datetime','instrument'])
        
        print(toqlib_df)

        toqlib_df.rename(columns={'datetime':'datetime','instrument':'instrument','score_y':'score'}, inplace = True)
        toqlib_df.drop(['score_x'],axis=1,inplace=True)

        toqlib_df.to_csv('qlibdataset333.csv')

        intsdfafsd=6


    def Topk(self):

        df_all = pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('./Database/Daily_adj_factor.csv',index_col=0,header=0)
        df_limit_all=pd.read_csv('./Database/Daily_stk_limit.csv',index_col=0,header=0)
        
        df_all=pd.merge(df_all, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='left', on=['ts_code','trade_date'])

        score_df = pd.read_csv('zzzzfackdatapred22.csv',index_col=0,header=0)
        #print(df_all)
        print(score_df)

        hold_all=100
        change_num=20
        account=100000000
        buy_pct=0.9
        Trans_cost=0.997        #千三

        codelist=pd.DataFrame(columns=('ts_code','lastprice','buy_amount','last_adj_factor'))
        codelist_buffer=pd.DataFrame(columns=('ts_code','lastprice','buy_amount','last_adj_factor'))
        #codelist=codelist.append([{'ts_code':1,'lastprice':1,'amount':1,'adjflag':1}])
        #print(codelist)

        score_df=score_df.sort_values(by=['trade_date'])
        datelist=score_df['trade_date'].unique()
        cur_hold_num=0
        print(datelist)
    
        days=0
        show3=[]
        for cur_date in datelist:

            #这里注意停牌的不包含在这个list中
            cur_df_all=df_all[df_all['trade_date'].isin([cur_date])]

            cur_score_df=score_df[score_df['trade_date'].isin([cur_date])]
            cur_merge_df=pd.merge(cur_df_all,cur_score_df, how='left', on=['trade_date','ts_code'])

            cur_merge_df['mix'].fillna(-99.99, inplace=True)

            #if(cur_date>20180102):
            #    cur_merge_df=cur_merge_df.to_csv("dsdf.csv")

            code_value_sum=0
            if(codelist.shape[0]>0):
                codelist_buffer=pd.merge(codelist,cur_merge_df, how='left', on=['ts_code'])

                codelist_buffer['adj_factor'].fillna(9999.99, inplace=True)
                codelist_buffer['close'].fillna(9999.99, inplace=True)
                

                codelist_buffer.loc[codelist_buffer['adj_factor']==9999.99,'adj_factor']=codelist_buffer['last_adj_factor']
                codelist_buffer.loc[codelist_buffer['close']==9999.99,'close']=codelist_buffer['lastprice']
                

                ###更新除权
                ##print(codelist_buffer.head(10))
                codelist_buffer.loc[:,'buy_amount']=codelist_buffer['buy_amount']*codelist_buffer['adj_factor']/codelist_buffer['last_adj_factor']

                #codelist_buffer.loc[:,'last_adj_factor']=codelist_buffer['adj_factor']
                #codelist_buffer.loc[:,'last_adj_factor']=codelist_buffer['adj_factor']

                #print(codelist_buffer.head(10))
                codelist.loc[:,'buy_amount']=codelist_buffer['buy_amount']
                codelist.loc[:,'last_adj_factor']=codelist_buffer['adj_factor']
                codelist.loc[:,'lastprice']=codelist_buffer['close']

                codelist_buffer['value']=codelist_buffer['buy_amount']*codelist_buffer['close']
                #codelist_buffer.reset_index(inplace=True,drop=True)
                
                #code_value_sum=codelist_buffer['value'].sum()

            #todo fillna
            #pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
            #print(cur_merge_df)

            #sell==========================
            sellto=hold_all-change_num
            sellnum=cur_hold_num-sellto

            if(sellnum>0):
                #todo can't sell limit
                codelist_buffer=codelist_buffer.sort_values(by=['mix'])
                #print(codelist_buffer['pct_chg'])
                #todo can't sell highstop
                selllist=codelist_buffer
                #selllist=selllist[selllist['pct_chg']<9]
                #排除跌停卖出
                selllist=selllist[selllist['close']!=selllist['down_limit']]

                selllist=selllist.head(sellnum)

                codelist.drop(codelist[codelist['ts_code'].isin(selllist['ts_code'])].index,inplace=True)

                codelist_buffer.drop(codelist_buffer[codelist_buffer['ts_code'].isin(selllist['ts_code'])].index,inplace=True)

                cur_hold_num-=selllist.shape[0]

                account=account+selllist['value'].sum()*Trans_cost

                sdfafa=1

            #buy==========================
            buyto=hold_all
            buynum=buyto-cur_hold_num

            if(buynum>0):

                buy_all_value=0
                if(codelist.shape[0]>0):
                    hold_code_sum=codelist_buffer['value'].sum()
                    buy_all_value=(account+hold_code_sum)*buy_pct-hold_code_sum

                else:
                    buy_all_value=account*buy_pct

                #when account too low then don't do anything
                if(buy_all_value<10000):
                    continue

                code_amount_buy=buy_all_value/buynum

                cur_merge_df=cur_merge_df.sort_values(by=['mix'])

                buylist=cur_merge_df
                #single code no repeat
                buylist=buylist[~buylist['ts_code'].isin(codelist['ts_code'])]

                #todo can't buy highstop
                #buylist=buylist[buylist['pct_chg']<4]
                buylist=buylist[buylist['close']!=buylist['up_limit']]
                #buylist=buylist[buylist['pct_chg']>-9]

                buylist=buylist.tail(buynum)

                buylist.loc[:,'buyuse']=code_amount_buy/buylist['close']
                #buylist['buyuse']=code_amount_buy/buylist['close']
                buylist.loc[:,'buyuse']=buylist['buyuse'].round(-2)
                buylist.loc[:,'buyuse']=buylist['buyuse'].astype(int)
                buylist['value']=buylist['close']*buylist['buyuse']

                #print(buylist)
                account=account-buylist['value'].sum()

                savebuylist=buylist[['ts_code','close','buyuse','adj_factor']]
                savebuylist.columns = ['ts_code','lastprice','buy_amount','last_adj_factor']

                codelist=codelist.append(savebuylist)
                #todo 这里因为下个循环drop会用到index如果不重新排序会造成问题，先这样改如果需要提升速度再进行修正
                codelist.reset_index(inplace=True,drop=True)
                cur_hold_num+=buynum
                sdfafa=1


            #print(codelist)
            #codelist_buffer=pd.merge(codelist,cur_merge_df, how='left', on=['ts_code'])
            bufferdf=codelist['buy_amount']*codelist['lastprice']
            #if(cur_date>20171018):
            #    print(codelist)
            #print(codelist)
            code_value_sum=bufferdf.sum()
            print(account+code_value_sum)
            print(cur_date)
            show3.append(account+code_value_sum)
            days+=1


        days=np.arange(1,days+1)
        plt.plot(days,show3,c='green',label="my model head10mean")

        plt.legend()

        plt.show()

        input()
        asdffd=1

    def qlib2CSZL(self):

        toqlib_df=pd.read_csv('./seesee.csv',header=0)       
        #toqlib_df2=pd.read_csv('./qlibdataset.csv',index_col=0,header=0)   

        toqlib_df.rename(columns={'datetime':'trade_date','instrument':'ts_code','score':'mix'}, inplace = True)
        print(toqlib_df.dtypes)
        print(toqlib_df)
        toqlib_df['trade_date'] = pd.to_datetime(toqlib_df['trade_date'], format='%Y-%m-%d')
        toqlib_df['trade_date']=toqlib_df['trade_date'].apply(lambda x: x.strftime('%Y%m%d'))

        toqlib_df['ts_codeL'] = toqlib_df['ts_code'].str[:2]
        toqlib_df['ts_codeR'] = toqlib_df['ts_code'].str[2:]

        toqlib_df['ts_codeR'] = toqlib_df['ts_codeR'].apply(lambda s: s+'.')

        toqlib_df['ts_code']=toqlib_df['ts_codeR'].str.cat(toqlib_df['ts_codeL'])

        toqlib_df.drop(['ts_codeL','ts_codeR'],axis=1,inplace=True)
        print(toqlib_df.dtypes)
        print(toqlib_df)

        toqlib_df.to_csv('qlibdataset333.csv')

        intsdfafsd=6

    def backTestingWithPredictDatas(self):
        #展示类
        dis=Display.Display()

        #dis.scatter(finalpath)
        dis.real_plot_create()

        dis.real_plot_show_plus()

        asfsdf=1

    def backtesting_forpara(self):

        import pandas as pd
        import datetime


        SuperGet=Dataget.Dataget()

        ###刷新资金量
        ##SuperGet.update_moneyflow('20190801','20190821')

        ##刷新数据库
        #SuperGet.updatedaily('20190801','20191101')

        ##刷新复权因子
        #SuperGet.updatedaily_adj_factor('20190101','20191101')

        ###刷新经济指标
        ##dataset_adj_train=SuperGet.updatedaily_long_factors('20100101','20190921')

        dayA='20130101'
        dayB='20160101'
        dayC='20160101'
        dayD='20200301'

        ##选择日期
        dataset_adj_train=SuperGet.getDataSet_adj_factor(dayA,dayB)
        dataset_adj_test=SuperGet.getDataSet_adj_factor(dayC,dayD)

        dataset_train=SuperGet.getDataSet(dayA,dayB)
        dataset_test=SuperGet.getDataSet(dayC,dayD)

        ##测试添加长期指标

        #dataset_long_train=SuperGet.getDataSet_long_factor('20121212','20180620')
        #dataset_long_test=SuperGet.getDataSet_long_factor('20180621','20200921')

        ##添加确定的stflag防止模型与实际情况的区别
        dataset_stk_limit_train=SuperGet.getDataSet_stk_limit(dayA,dayB)
        dataset_stk_limit_test=SuperGet.getDataSet_stk_limit(dayC,dayD)

        ##测试添加资金量指标
        dataset_moneyflow_train=SuperGet.getDataSet_moneyflow(dayA,dayB)
        dataset_moneyflow_test=SuperGet.getDataSet_moneyflow(dayC,dayD)

        #选择特征工程
        #cur_fe=FE.FEg30e()
        #cur_fe=FE.FEg30ea()
        cur_fe=FE.FE_2_b()
        #cur_fe=FE.FEg30r()

        FE_train=cur_fe.create(dataset_train,dataset_adj_train,dataset_stk_limit_train,dataset_moneyflow_train)
        FE_test=cur_fe.create(dataset_test,dataset_adj_test,dataset_stk_limit_test,dataset_moneyflow_test)
        #FE_train=cur_fe.create(dataset_train,dataset_adj_train,dataset_moneyflow_train)
        #FE_test=cur_fe.create(dataset_test,dataset_adj_test,dataset_moneyflow_test)


        #选择模型
        cur_model=Models.LGBmodel()
        #训练模型
        cur_model_done=cur_model.train(FE_train)
        #进行回测
        finalpath=cur_model.predict(FE_test,cur_model_done)
    
        #展示类
        dis=Display.Display()

        bufferlist=dis.parafirst(finalpath)

        #+++++++++++++++++++++++#

        multpare=[-30,0,0,0,0,0,0,0,0,0,-1]
        #multpare=[-30,0,0,0,0,0,0,0,0,0,0,0,0,0,0,-1]
        cur_base=0
        cur_index=0

        name=['0','1','2','3','4','5','6','7','8','9','Score']
        #name=['0','1','2','3','4','Score']

        zzzz=pd.DataFrame(columns=name)
        nowTime=datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        savepathz="para"+nowTime+".csv"
        zzzz.to_csv(savepathz,encoding='gbk')
        ct=0
        changernum=-20

        #Y=[-17,8,-9,0,0,0,0,-1,20,25]
        Y=[-12,-6,-3,-2,-1,1,2,3,6,12]
        #Y=[-18,-14,-11,0,0,0,0,-27,-11,25]
        #Y=[-18,-10,-17,0,0,0,0,-15,-11,25]
        while(1):
            getcsv=pd.read_csv(savepathz,index_col=0,header=0)
            xxx=self._reproduce(10)
            changernum=self._reproduce2(changernum)
            multpare[0]=xxx[0]
            multpare[1]=xxx[1]
            multpare[2]=xxx[2]
            multpare[3]=xxx[3]
            multpare[4]=xxx[4]
            multpare[5]=xxx[5]
            multpare[6]=xxx[6]
            multpare[7]=xxx[7]
            multpare[8]=xxx[8]
            multpare[9]=xxx[9]
            #multpare[9]=changernum
            #multpare[10]=35
            #multpare[11]=0
            #multpare[12]=xxx[3]
            #multpare[13]=xxx[4]
            ##multpare[9]=xxx[5]
            #multpare[14]=35
            para=multpare[:10]
            multpare[10]=dis.paramain(bufferlist,para)
            plus_index=getcsv.shape[0]
            getcsv.loc[plus_index]=multpare

            getcsv.to_csv(savepathz,encoding='gbk')
            print(ct)
            ct+=1

        sdfafsdfas=1

    def _reproduce(self,counter):
        #随机生成
        import random

        multpare=list(range(0, counter))

        while(counter):
            multpare[counter-1] = random.randint(-50,50)
            counter-=1
        return multpare
    def _reproduce2(self,inputnum):
        #顺序生成
        inputnum+=1
        return inputnum