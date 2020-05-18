#coding=utf-8

import Display
import FeatureEnvironment as FE
import Dataget
import Models


class BackTesting(object):
    """description of class"""

    def backTesting(self):

        SuperGet=Dataget.Dataget()

        ####刷新资金量
        #SuperGet.update_moneyflow('20130101','20200516')

        ##刷新数据库
        #SuperGet.updatedaily('20130101','20200516')

        ##刷新复权因子
        #SuperGet.updatedaily_adj_factor('20130101','20200516')

        #刷新经济指标
        dataset_adj_train=SuperGet.updatedaily_long_factors('20200101','20200516')

        ##刷新个股波动范围
        #SuperGet.update_stk_limit('20130101','20200516')

        dayA='20130701'
        dayB='20180101'
        dayC='20180101'
        dayD='20200516'

        ##选择日期
        dataset_adj_train=SuperGet.getDataSet_adj_factor(dayA,dayB)
        dataset_adj_test=SuperGet.getDataSet_adj_factor(dayC,dayD)

        dataset_train=SuperGet.getDataSet(dayA,dayB)
        dataset_test=SuperGet.getDataSet(dayC,dayD)

        #测试添加长期指标

        dataset_long_train=SuperGet.getDataSet_long_factor(dayA,dayB)
        dataset_long_test=SuperGet.getDataSet_long_factor(dayC,dayD)

        ##添加确定的stflag防止模型与实际情况的区别
        dataset_stk_limit_train=SuperGet.getDataSet_stk_limit(dayA,dayB)
        dataset_stk_limit_test=SuperGet.getDataSet_stk_limit(dayC,dayD)

        ##测试添加资金量指标
        dataset_moneyflow_train=SuperGet.getDataSet_moneyflow(dayA,dayB)
        dataset_moneyflow_test=SuperGet.getDataSet_moneyflow(dayC,dayD)

        #选择特征工程

        cur_fe=FE.FEg30eo()
        #cur_fe=FE.FEg30ed()
        #cur_fe=FE.FE_2_b()
        #cur_fe=FE.FEg30r()

        FE_train=cur_fe.create(dataset_train,dataset_adj_train,dataset_stk_limit_train,dataset_moneyflow_train,dataset_long_train)
        FE_test=cur_fe.create(dataset_test,dataset_adj_test,dataset_stk_limit_test,dataset_moneyflow_test,dataset_long_test)
        #FE_train=cur_fe.create(dataset_train,dataset_adj_train,dataset_moneyflow_train)
        #FE_test=cur_fe.create(dataset_test,dataset_adj_test,dataset_moneyflow_test)

        #选择模型
        cur_model=Models.LGBmodel()
        #cur_model=models.LGBmodel_highstop()
        #训练模型
        cur_model_done=cur_model.train(FE_train)
        #进行回测
        finalpath=cur_model.predict(FE_test,cur_model_done)
    
        #展示类
        dis=Display.Display()

        #dis.scatter(finalpath)
        dis.plotall(finalpath)


        sdfsdf=1

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