#coding=utf-8

import Display
import FeatureEnvironment as FE
import Dataget
import Models


class BackTesting(object):
    """description of class"""

    def backTesting(self):

        SuperGet=Dataget.Dataget()

        ###刷新资金量
        #SuperGet.update_moneyflow('20130101','20200114')

        ##刷新数据库
        #SuperGet.updatedaily('20130101','20200114')

        ##刷新复权因子
        #SuperGet.updatedaily_adj_factor('20130101','20200114')

        ##刷新经济指标
        #dataset_adj_train=SuperGet.updatedaily_long_factors('20130101','20200114')

        ###刷新个股波动范围
        #SuperGet.update_stk_limit('20130101','20200114')


        ##选择日期
        dataset_adj_train=SuperGet.getDataSet_adj_factor('20130101','20170101')
        dataset_adj_test=SuperGet.getDataSet_adj_factor('20170101','20200114')

        dataset_train=SuperGet.getDataSet('20130101','20170101')
        dataset_test=SuperGet.getDataSet('20170101','20200114')

        #测#试添加长期指标

        #dataset_long_train=SuperGet.getDataSet_long_factor('20121212','20170620')
        #dataset_long_test=SuperGet.getDataSet_long_factor('20170621','20190921')

        ##添加确定的stflag防止模型与实际情况的区别
        dataset_stk_limit_train=SuperGet.getDataSet_stk_limit('20130101','20170101')
        dataset_stk_limit_test=SuperGet.getDataSet_stk_limit('20170101','20200114')

        ##测试添加资金量指标
        dataset_moneyflow_train=SuperGet.getDataSet_moneyflow('20130101','20170101')
        dataset_moneyflow_test=SuperGet.getDataSet_moneyflow('20170101','20200114')

        #选择特征工程
        #cur_fe=FE.FE3()
        #cur_fe=FE.FEg30()
        #cur_fe=FE.FEh30f()
        #cur_fe=FE.FEg30b()
        cur_fe=FE.FEg30e()
        #cur_fe=FE.FEg30q()

        FE_test=cur_fe.create(dataset_train,dataset_adj_train,dataset_stk_limit_train,dataset_moneyflow_train)
        FE_train=cur_fe.create(dataset_test,dataset_adj_test,dataset_stk_limit_test,dataset_moneyflow_test)
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



        ##选择日期
        dataset_adj_train=SuperGet.getDataSet_adj_factor('20130101','20170101')
        dataset_adj_test=SuperGet.getDataSet_adj_factor('20170101','20200101')

        dataset_train=SuperGet.getDataSet('20130101','20170101')
        dataset_test=SuperGet.getDataSet('20170101','20200101')

        #测试添加长期指标

        #dataset_long_train=SuperGet.getDataSet_long_factor('20121212','20160620')
        #dataset_long_test=SuperGet.getDataSet_long_factor('20160621','20190921')

        #添加确定的stflag防止模型与实际情况的区别
        dataset_stk_limit_train=SuperGet.getDataSet_stk_limit('20130101','20170101')
        dataset_stk_limit_test=SuperGet.getDataSet_stk_limit('20170101','20200101')

        ##测试添加资金量指标
        #dataset_moneyflow_train=SuperGet.getDataSet_moneyflow('20130101','20170101')
        #dataset_moneyflow_test=SuperGet.getDataSet_moneyflow('20170101','20200101')

        #选择特征工程
        #cur_fe=FE.FE3()
        #cur_fe=FE.FEg30()
        cur_fe=FE.FEg30b()

        FE_train=cur_fe.create(dataset_train,dataset_adj_train,dataset_stk_limit_train)
        FE_test=cur_fe.create(dataset_test,dataset_adj_test,dataset_stk_limit_test)
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
            multpare[0]=-12
            multpare[1]=-6
            multpare[2]=-3
            multpare[3]=-2
            multpare[4]=-1
            multpare[5]=1
            multpare[6]=2
            multpare[7]=3
            multpare[8]=6
            multpare[9]=changernum
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
        import random

        multpare=list(range(0, counter))

        while(counter):
            multpare[counter-1] = random.randint(-50,50)
            counter-=1
        return multpare
    def _reproduce2(self,inputnum):
        inputnum+=1
        return inputnum