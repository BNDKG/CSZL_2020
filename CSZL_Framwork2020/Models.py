#coding=utf-8
import abc
import pandas as pd
import numpy as np
#import keras

#from keras import models
#from keras import layers

import lightgbm as lgb
from sklearn import datasets
from sklearn.model_selection import StratifiedKFold
from sklearn.externals import joblib
#from sklearn.linear_model import LogisticRegressionCV,LinearRegression
#from keras.utils import np_utils
#from keras.models import load_model

#from sklearn.preprocessing import OneHotEncoder

import os


class BaseModel(object):
    """description of class"""

    def __init__(self):
        pass

    #@abc.abstractmethod
    def predict(self, Predictpath,modelpath):
        """Subclass must implement this."""
        #print (self.__class__.__name__)
        (filepath, tempfilename) = os.path.split(Predictpath)
        (filename, extension) = os.path.splitext(tempfilename)

        (filepath2, tempfilename2) = os.path.split(modelpath)
        (filename2, extension2) = os.path.splitext(tempfilename2)
        


        bufferstringoutput=filepath+'/'+filename+'_'+filename2+'.csv'
        bufferstringoutput2=filepath+'/'+filename+'_'+filename2+'0.csv'
        if(os.path.exists(bufferstringoutput)==False and os.path.exists(bufferstringoutput2)==False):    

            df_all=pd.read_csv(Predictpath,index_col=0,header=0)
            #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
            self.core_predict(df_all,modelpath,bufferstringoutput)
            #df_all.to_csv(bufferstringoutput)

        return bufferstringoutput

    def train(self,DataSetName):
        #print (self.__class__.__name__)
        (filepath, tempfilename) = os.path.split(DataSetName)
        (filename, extension) = os.path.splitext(tempfilename)

        bufferstringoutput=filepath+'/'+filename+'_'+self.__class__.__name__+'.pkl'
        bufferstringoutput2=filepath+'/'+filename+'_'+self.__class__.__name__+'0.pkl'
        if(os.path.exists(bufferstringoutput)==False and os.path.exists(bufferstringoutput2)==False):    

            df_all=pd.read_csv(DataSetName,index_col=0,header=0)
            #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
            self.core_train(df_all,bufferstringoutput)
            #df_all.to_csv(bufferstringoutput)

        return bufferstringoutput


    def core_train(self, train,savepath):
        pass

    def core_predict(self,train,modelpath,savepath):
        pass

class LGBmodel_20(BaseModel):


    def core_predict(self,train,modelpath,savepath):
        """Subclass must implement this."""

        new_train_times=4

        train=train.reset_index(drop=True)
        train2=train.copy(deep=True)

        y_train = np.array(train['tomorrow_chg_rank'])
        train.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1,inplace=True)

        for counter in range(new_train_times):
            modelpath_new=os.path.splitext(modelpath)[0]+str(counter)+".pkl"

            lgb_model = joblib.load(modelpath_new)

            dsadwd=lgb_model.feature_importances_
            print(dsadwd)
            pred_test = lgb_model.predict_proba(train)

            data1 = pd.DataFrame(pred_test)

            data1['mix']=0
            #multlist=[-12,-5,-3,-2,-1.5,-1,-0.75,-0.5,-0.25,0,0,0.25,0.5,0.75,1,1.5,2,3,5,12]
            #multlist=[-10,-3,-2,-1,0,0,1,2,3,10]
            multlist=[-12,-6,-3,-2,-1,0,0,0,0,0,0,0,0,0,0,1,2,3,6,12]
            #multlist=[-8,-8,0,7,12]

            for i in range(20):
                buffer=data1[i]*multlist[i]
                data1['mix']=data1['mix']+buffer

            train3=train2.join(data1)
    
            print(train3)

            savepath_new=os.path.splitext(savepath)[0]+str(counter)+".csv"
            train3.to_csv(savepath_new)

        return 2

    def core_train(self,train,savepath):

        #readstring='ztrain'+year+'.csv'
        
        #readstring=path+'.csv'

        ##train=pd.read_csv(readstring,index_col=0,header=0,nrows=10000)
        #train=pd.read_csv(readstring,index_col=0,header=0)
        train=train.reset_index(drop=True)

        y_train = np.array(train['tomorrow_chg_rank'])
        train.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1,inplace=True)


        #x=train.shape[1]
        #y=train.shape[0]
        #Random01=np.random.rand(y, x)
        #Random01[Random01<0.1]=-20
        #Random01[Random01>0]=0
        ##print(Random01)

        ##print(train)
        #train=train+Random01
        #train[Random01<0]=3
        #print(train)
        train_ids = train.index.tolist()

        splitno=int(len(train_ids)*0.70)
        splitno10=int(len(train_ids)*0.15)
        splitno11=int(len(train_ids)*0.85)
        splitno20=int(len(train_ids)*0.35)
        splitno21=int(len(train_ids)*0.65)

        train_index_list=[]
        test_index_list=[]
        #1
        train_index_list.append(train_ids[:splitno])
        test_index_list.append(train_ids[(splitno+10000):])
        #2
        train_index_list.append(train_ids[(len(train_ids)-splitno):])
        test_index_list.append(train_ids[:(len(train_ids)-splitno-10000)])
        #3
        train_index_list.append(train_ids[splitno10:splitno11])
        buffer=train_ids[:(splitno10-10000)]
        buffer.extend(train_ids[(splitno11+10000):])
        test_index_list.append(buffer)
        #4
        buffer=train_ids[:(splitno20)]
        buffer.extend(train_ids[(splitno21):])
        train_index_list.append(buffer)
        test_index_list.append(train_ids[(splitno20+10000):(splitno21-10000)])

        train=train.values

        new_train_times=4

        for counter in range(new_train_times):
       
            X_fit, X_val = train[train_index_list[counter]],train[test_index_list[counter]]
            y_fit, y_val = y_train[train_index_list[counter]], y_train[test_index_list[counter]]

            sample_weights = np.zeros(len(y_fit))
            # your own weight corresponding here:
            sample_weights[y_fit==0] = 2                                                                                                       
            sample_weights[y_fit==1] = 1                                                                                                          
            sample_weights[y_fit==2] = 1                                                                                                            
            sample_weights[y_fit==3] = 1                                                                                                            
            sample_weights[y_fit==4] = 1                                                                                                            
            sample_weights[y_fit==5] = 1                                                                                                            
            sample_weights[y_fit==6] = 1                                                                                                            
            sample_weights[y_fit==7] = 1                                                                                                           
            sample_weights[y_fit==8] = 1                                                                                                            
            sample_weights[y_fit==9] = 1                                                                                                           
            sample_weights[y_fit==10] = 1                                                                                                        
            sample_weights[y_fit==11] = 1
            sample_weights[y_fit==12] = 1                                                                                                             
            sample_weights[y_fit==13] = 1                                                                                                            
            sample_weights[y_fit==14] = 1                                                                                                             
            sample_weights[y_fit==15] = 1                                                                                                             
            sample_weights[y_fit==16] = 1                                                                                                             
            sample_weights[y_fit==17] = 1                                                                                                             
            sample_weights[y_fit==18] = 1                                                                                                             
            sample_weights[y_fit==19] = 2                                                                                                          

            sample_weight_vals = np.zeros(len(y_val))
            # your own weight corresponding here:
            sample_weight_vals[y_val==0] = 2                                                                                                      
            sample_weight_vals[y_val==1] = 1                                                                                                          
            sample_weight_vals[y_val==2] = 1                                                                                                            
            sample_weight_vals[y_val==3] = 1                                                                                                            
            sample_weight_vals[y_val==4] = 1                                                                                                            
            sample_weight_vals[y_val==5] = 1                                                                                                            
            sample_weight_vals[y_val==6] = 1                                                                                                            
            sample_weight_vals[y_val==7] = 1                                                                                                           
            sample_weight_vals[y_val==8] = 1                                                                                                            
            sample_weight_vals[y_val==9] = 1                                                                                                           
            sample_weight_vals[y_val==10] = 1                                                                                                        
            sample_weight_vals[y_val==11] = 1
            sample_weight_vals[y_val==12] = 1                                                                                                             
            sample_weight_vals[y_val==13] = 1                                                                                                            
            sample_weight_vals[y_val==14] = 1                                                                                                             
            sample_weight_vals[y_val==15] = 1                                                                                                             
            sample_weight_vals[y_val==16] = 1                                                                                                             
            sample_weight_vals[y_val==17] = 1                                                                                                             
            sample_weight_vals[y_val==18] = 1                                                                                                         
            sample_weight_vals[y_val==19] = 2   

            #print(y_fit_weight)

            lgb_model = lgb.LGBMClassifier(max_depth=-1,
                                            n_estimators=300,
                                            learning_rate=0.05,
                                            num_leaves=2**8-1,
                                            colsample_bytree=0.6,
                                            objective='multiclass', 
                                            num_class=20,
                                            n_jobs=-1)
                                   

            #lgb_model.fit(X_fit, y_fit, eval_metric='multi_error',
            #                eval_set=[(X_val, y_val)], 
            #                verbose=100, early_stopping_rounds=1000)
            lgb_model.fit(X_fit, y_fit, eval_metric='multi_error',
                            eval_set=[(X_val, y_val)], 
                            sample_weight=sample_weights,
                            eval_sample_weight=[sample_weight_vals],
                            verbose=100, early_stopping_rounds=None)
     
            savepath_new=os.path.splitext(savepath)[0]+str(counter)+".pkl"
            joblib.dump(lgb_model,savepath_new)           




        #X_train,X_test,y_train,y_test=train_test_split(iris.data,iris.target,test_size=0.3)        

        return 2
        #return super().train(dataset)

    def real_lgb_predict(self,modelpath,outputname):
        readstring='today_train.csv'

        #train=pd.read_csv(readstring,index_col=0,header=0,nrows=10000)
        train=pd.read_csv(readstring,index_col=0,header=0)
        train=train.reset_index(drop=True)
        train2=train.copy(deep=True)

        #train.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1,inplace=True)
        train.drop(['ts_code','trade_date'],axis=1,inplace=True)


        lgb_model = joblib.load(modelpath)

        dsadwd=lgb_model.feature_importances_

        pred_test = lgb_model.predict_proba(train)

        data1 = pd.DataFrame(pred_test)

        data1['mix']=0
        #multlist=[-10,-3,-2,-1,0,0,1,2,3,10]
        #multlist=[-12,-6,-3,-2,-1,0,0,0,0,0,0,0,0,0,0,1,2,3,6,12]
        multlist=[-12,-6,-3,-2,-1,0,0,0,0,0,0,1,1,1,1,1,2,3,7,14]
        
        #[-12,-8,-3,-2,-1,1,2,3,10,18]
        #[-7,-4,-3,-2,-1,1,2,3,4,12]

        for i in range(20):
            buffer=data1[i]*multlist[i]
            data1['mix']=data1['mix']+buffer

        data1['mix']=data1['mix'].rank(ascending=True,pct=False,method='first')
        data1['mix']=data1['mix']/1000
        train2=train2.join(data1)
    
        print(train2)
        
        train2.to_csv(outputname)

        dawd=1

class Networkmodel_20(BaseModel):

    def core_predict(self,train,modelpath,savepath):
        """Subclass must implement this."""

        new_train_times=4

        train=train.reset_index(drop=True)
        train2=train.copy(deep=True)

        y_train = np.array(train['tomorrow_chg_rank'])
        train.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1,inplace=True)

        for counter in range(new_train_times):
            modelpath_new=os.path.splitext(modelpath)[0]+str(counter)+".h5"

            Network_model = load_model(modelpath_new)

            #dsadwd=Network_model.feature_importances_
            #print(dsadwd)
            pred_test = Network_model.predict(train.values)

            data1 = pd.DataFrame(pred_test)

            data1['mix']=0
            #multlist=[-12,-5,-3,-2,-1.5,-1,-0.75,-0.5,-0.25,0,0,0.25,0.5,0.75,1,1.5,2,3,5,12]
            #multlist=[-10,-3,-2,-1,0,0,1,2,3,10]
            multlist=[-12,-6,-3,-2,-1,0,0,0,0,0,0,0,0,0,0,1,2,3,6,12]
            #multlist=[-8,-8,0,7,12]

            for i in range(20):
                buffer=data1[i]*multlist[i]
                data1['mix']=data1['mix']+buffer

            train3=train2.join(data1)
    
            print(train3)

            savepath_new=os.path.splitext(savepath)[0]+str(counter)+".csv"
            train3.to_csv(savepath_new)

        return 2

    def core_train(self,train,savepath):

        #readstring='ztrain'+year+'.csv'
        
        #readstring=path+'.csv'

        ##train=pd.read_csv(readstring,index_col=0,header=0,nrows=10000)
        #train=pd.read_csv(readstring,index_col=0,header=0)
        train=train.reset_index(drop=True)

        y_train = np.array(train['tomorrow_chg_rank'])
        train.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1,inplace=True)

        #x=train.shape[1]
        #y=train.shape[0]
        #Random01=np.random.rand(y, x)
        #Random01[Random01<0.1]=-20
        #Random01[Random01>0]=0
        ##print(Random01)

        ##print(train)
        #train=train+Random01
        #train[Random01<0]=3
        print(train)

        #ONEHOT_COLUMNS = ["open", "yesterday_3pst_amount_rank_12"]

        #local_ohe = LocalOneHotEncoder(ONEHOT_COLUMNS)
        #local_ohe.fit(train)
        #oht_df = local_ohe.transform(train)

        #oht_df.head()


        train_ids = train.index.tolist()

        splitno=int(len(train_ids)*0.70)
        splitno10=int(len(train_ids)*0.15)
        splitno11=int(len(train_ids)*0.85)
        splitno20=int(len(train_ids)*0.35)
        splitno21=int(len(train_ids)*0.65)

        train_index_list=[]
        test_index_list=[]
        #1
        train_index_list.append(train_ids[:splitno])
        test_index_list.append(train_ids[(splitno+10000):])
        #2
        train_index_list.append(train_ids[(len(train_ids)-splitno):])
        test_index_list.append(train_ids[:(len(train_ids)-splitno-10000)])
        #3
        train_index_list.append(train_ids[splitno10:splitno11])
        buffer=train_ids[:(splitno10-10000)]
        buffer.extend(train_ids[(splitno11+10000):])
        test_index_list.append(buffer)
        #4
        buffer=train_ids[:(splitno20)]
        buffer.extend(train_ids[(splitno21):])
        train_index_list.append(buffer)
        test_index_list.append(train_ids[(splitno20+10000):(splitno21-10000)])

        train=train.values
        print(train)
        new_train_times=4

        for counter in range(new_train_times):
       
            X_fit, X_val = train[train_index_list[counter]],train[test_index_list[counter]]
            y_fit, y_val = y_train[train_index_list[counter]], y_train[test_index_list[counter]]

            y_fit=np_utils.to_categorical(y_fit, 20)
            y_val=np_utils.to_categorical(y_val, 20)


            #X_fit=self.onehotfast(X_fit)
            #X_val=self.onehotfast(X_val)
            #X_fitt=X_fit.T
            #X_fitopt=[]
            #for i in range(X_fit.shape[1]):

            #    X_fitz=X_fitt[i]
            #    X_fitz2=X_fitz.T
            #    X_fitc=np_utils.to_categorical(X_fitz2, 20)
            #    if i==0:
            #        X_fitopt=X_fitc
            #    else:
            #        X_fitopt=np.hstack((X_fitopt,X_fitc))
            #    print(X_fitopt)

            Netmodel=models.Sequential()
            Netmodel.add(layers.Dense(64,activation='relu', input_shape=(33,)))
            Netmodel.add(layers.Dense(64,activation='relu'))
            Netmodel.add(layers.Dense(64,activation='relu'))
            Netmodel.add(layers.Dense(20,activation='softmax'))

            Netmodel.compile(optimizer='RMSProp',loss='categorical_crossentropy',metrics=['accuracy'])

            history=Netmodel.fit(X_fit,y_fit,batch_size=128,epochs=20,validation_data=(X_val,y_val))

            #opt=Netmodel.predict(X_val)  

            #lgb_model = lgb.LGBMClassifier(max_depth=-1,
            #                                n_estimators=300,
            #                                learning_rate=0.05,
            #                                num_leaves=2**8-1,
            #                                colsample_bytree=0.6,
            #                                objective='multiclass', 
            #                                num_class=20,
            #                                n_jobs=-1)
                                   

            ##lgb_model.fit(X_fit, y_fit, eval_metric='multi_error',
            ##                eval_set=[(X_val, y_val)], 
            ##                verbose=100, early_stopping_rounds=1000)
            #lgb_model.fit(X_fit, y_fit, eval_metric='multi_error',
            #                eval_set=[(X_val, y_val)], 
            #                verbose=100, early_stopping_rounds=None)
     
            savepath_new=os.path.splitext(savepath)[0]+str(counter)+".h5"
            #joblib.dump(Netmodel,savepath_new)           
            Netmodel.save(savepath_new)



        #X_train,X_test,y_train,y_test=train_test_split(iris.data,iris.target,test_size=0.3)        

        return 2
        #return super().train(dataset)

    def onehotfast(self,X_fit):
        X_fitt=X_fit.T
        X_fitopt=[]
        for i in range(X_fit.shape[1]):

            X_fitz=X_fitt[i]
            X_fitz2=X_fitz.T
            X_fitc=np_utils.to_categorical(X_fitz2, 20)
            if i==0:
                X_fitopt=X_fitc
            else:
                X_fitopt=np.hstack((X_fitopt,X_fitc))
            
        return X_fitopt

    def real_lgb_predict(self,modelpath,outputname):
        readstring='today_train.csv'

        #train=pd.read_csv(readstring,index_col=0,header=0,nrows=10000)
        train=pd.read_csv(readstring,index_col=0,header=0)
        train=train.reset_index(drop=True)
        train2=train.copy(deep=True)

        #train.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1,inplace=True)
        train.drop(['ts_code','trade_date'],axis=1,inplace=True)


        lgb_model = joblib.load(modelpath)

        dsadwd=lgb_model.feature_importances_

        pred_test = lgb_model.predict_proba(train)

        data1 = pd.DataFrame(pred_test)

        data1['mix']=0
        #multlist=[-10,-3,-2,-1,0,0,1,2,3,10]
        multlist=[-12,-6,-3,-2,-1,0,0,0,0,0,0,0,0,0,0,1,2,3,6,12]

        #[-12,-8,-3,-2,-1,1,2,3,10,18]
        #[-7,-4,-3,-2,-1,1,2,3,4,12]

        for i in range(20):
            buffer=data1[i]*multlist[i]
            data1['mix']=data1['mix']+buffer

        data1['mix']=data1['mix'].rank(ascending=True,pct=False,method='first')
        data1['mix']=data1['mix']/1000
        train2=train2.join(data1)
    
        print(train2)
        
        train2.to_csv(outputname)

        dawd=1

class LGBmodel_reg(BaseModel):


    def core_predict_old(self,train,modelpath,savepath):
        """Subclass must implement this."""

        #readstring='ztrain'+year+'.csv'

        #train=pd.read_csv(readstring,index_col=0,header=0,nrows=10000)
        #train=pd.read_csv(readstring,index_col=0,header=0)
        train=train.reset_index(drop=True)
        train2=train.copy(deep=True)


        y_train = np.array(train['tomorrow_chg_rank'])
        train.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1,inplace=True)

        #corrmat = train.corr()
        #f, ax = plt.subplots(figsize=(12, 9))
        #sns.heatmap(corrmat, vmax=.8, square=True);
        #plt.show()

        lgb_model = joblib.load(modelpath)

        dsadwd=lgb_model.feature_importances_

        pred_test = lgb_model.predict(train,num_iteration=lgb_model.best_iteration_)

        data1 = pd.DataFrame(pred_test)

        data1.to_csv(outputname)

        data1['mix']=0
        #multlist=[-12,-5,-3,-2,-1.5,-1,-0.75,-0.5,-0.25,0,0,0.25,0.5,0.75,1,1.5,2,3,5,12]
        #multlist=[-10,-3,-2,-1,0,0,1,2,3,10]
        multlist=[-8,-8,-3,-2,-1,0,0,0,0,0,1,2,3,7,12]

        for i in range(10):
            buffer=data1[i]*multlist[i]
            data1['mix']=data1['mix']+buffer

        train2=train2.join(data1)
    
        print(train2)

        train2.to_csv(savepath)

        return 2

    def core_predict(self,train,modelpath,savepath):
        """Subclass must implement this."""

        new_train_times=4

        train=train.reset_index(drop=True)
        train2=train.copy(deep=True)

        y_train = np.array(train['tomorrow_chg_rank'])
        train.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1,inplace=True)

        for counter in range(new_train_times):
            modelpath_new=os.path.splitext(modelpath)[0]+str(counter)+".pkl"
            savepath_new=os.path.splitext(savepath)[0]+str(counter)+".csv"

            lgb_model = joblib.load(modelpath_new)

            dsadwd=lgb_model.feature_importances_
            print(dsadwd)
            pred_test = lgb_model.predict(train,num_iteration=lgb_model.best_iteration_)

            data1 = pd.DataFrame(pred_test)
            #data1.to_csv(savepath_new)
            #data1['mix']=0
            #multlist=[-12,-5,-3,-2,-1.5,-1,-0.75,-0.5,-0.25,0,0,0.25,0.5,0.75,1,1.5,2,3,5,12]
            #multlist=[-10,-3,-2,-1,0,0,1,2,3,10]
            #multlist=[-8,-8,-3,-2,-1,1,2,3,7,12]
            #multlist=[-8,-8,0,7,12]

      

            #for i in range(10):
            #    buffer=data1[i]*multlist[i]
            #    data1['mix']=data1['mix']+buffer

            train3=train2.join(data1)
    
            print(train3)

            
            train3.to_csv(savepath_new)

        return 2

    def core_train_old(self,train,savepath):

        #readstring='ztrain'+year+'.csv'
        
        #readstring=path+'.csv'

        ##train=pd.read_csv(readstring,index_col=0,header=0,nrows=10000)
        #train=pd.read_csv(readstring,index_col=0,header=0)
        train=train.reset_index(drop=True)

        y_train = np.array(train['tomorrow_chg_rank'])
        train.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1,inplace=True)


        train_ids = train.index.tolist()

        skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=12)
        skf.get_n_splits(train_ids, y_train)

        train=train.values

        counter=0

        for train_index, test_index in skf.split(train_ids, y_train):
        
            X_fit, X_val = train[train_index],train[test_index]
            y_fit, y_val = y_train[train_index], y_train[test_index]

            lgb_model = lgb.LGBMRegressor(max_depth=-1,
                                           n_estimators=500,
                                           learning_rate=0.05,
                                           num_leaves=2**8-1,
                                           colsample_bytree=0.6,
                                           objective='regression', 
                                           num_class=21,
                                           n_jobs=-1)
                                   

            lgb_model.fit(X_fit, y_fit, eval_metric='l2',
                          eval_set=[(X_val, y_val)], 
                          verbose=100, early_stopping_rounds=1000)
     
            joblib.dump(lgb_model,savepath)           
            #joblib.dump(lgb_model,"lgbnew_model"+str(counter)+".pkl")
            break

            #lgb_model = joblib.load(savepath)

            #pred_test = lgb_model.predict_proba(X_val)

            #np.set_printoptions(threshold=np.inf) 

            #pd.set_option('display.max_rows', 10000)  # 设置显示最大行
            #pd.set_option('display.max_columns', None)
            #print(pred_test)

            #data1 = pd.DataFrame(pred_test)
            #data1.to_csv('data1.csv')

            #gc.collect()
            counter += 1    
            #Stop fitting to prevent time limit error
            if counter == 4 : break


        #X_train,X_test,y_train,y_test=train_test_split(iris.data,iris.target,test_size=0.3)        

        return 2

    def core_train(self,train,savepath):

        #readstring='ztrain'+year+'.csv'
        
        #readstring=path+'.csv'

        ##train=pd.read_csv(readstring,index_col=0,header=0,nrows=10000)
        #train=pd.read_csv(readstring,index_col=0,header=0)
        train=train.reset_index(drop=True)

        y_train = np.array(train['tomorrow_chg_rank'])
        train.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1,inplace=True)

        #x=train.shape[1]
        #y=train.shape[0]
        #Random01=np.random.rand(y, x)
        #Random01[Random01<0.1]=-20
        #Random01[Random01>0]=0
        ##print(Random01)

        ##print(train)
        #train=train+Random01
        #train[Random01<0]=3
        #print(train)
        train_ids = train.index.tolist()

        splitno=int(len(train_ids)*0.70)
        splitno10=int(len(train_ids)*0.15)
        splitno11=int(len(train_ids)*0.85)
        splitno20=int(len(train_ids)*0.35)
        splitno21=int(len(train_ids)*0.65)

        train_index_list=[]
        test_index_list=[]
        #1
        train_index_list.append(train_ids[:splitno])
        test_index_list.append(train_ids[(splitno+10000):])
        #2
        train_index_list.append(train_ids[(len(train_ids)-splitno):])
        test_index_list.append(train_ids[:(len(train_ids)-splitno-10000)])
        #3
        train_index_list.append(train_ids[splitno10:splitno11])
        buffer=train_ids[:(splitno10-10000)]
        buffer.extend(train_ids[(splitno11+10000):])
        test_index_list.append(buffer)
        #4
        buffer=train_ids[:(splitno20)]
        buffer.extend(train_ids[(splitno21):])
        train_index_list.append(buffer)
        test_index_list.append(train_ids[(splitno20+10000):(splitno21-10000)])

        train=train.values

        new_train_times=4

        for counter in range(new_train_times):
       
            X_fit, X_val = train[train_index_list[counter]],train[test_index_list[counter]]
            y_fit, y_val = y_train[train_index_list[counter]], y_train[test_index_list[counter]]

            lgb_model = lgb.LGBMRegressor(max_depth=-1,
                                            n_estimators=500,
                                            learning_rate=0.05,
                                            num_leaves=2**8-1,
                                            colsample_bytree=0.6,
                                            objective='regression', 
                                            #num_class=10,
                                            n_jobs=-1)
                                   

            #lgb_model.fit(X_fit, y_fit, eval_metric='multi_error',
            #                eval_set=[(X_val, y_val)], 
            #                verbose=100, early_stopping_rounds=1000)
            lgb_model.fit(X_fit, y_fit, eval_metric='l2',
                            eval_set=[(X_val, y_val)], 
                            verbose=100, early_stopping_rounds=None)
     
            savepath_new=os.path.splitext(savepath)[0]+str(counter)+".pkl"
            joblib.dump(lgb_model,savepath_new)           

        #X_train,X_test,y_train,y_test=train_test_split(iris.data,iris.target,test_size=0.3)        

        return 2
        #return super().train(dataset)

    def real_lgb_predict(self,modelpath,outputname):
        readstring='today_train.csv'

        #train=pd.read_csv(readstring,index_col=0,header=0,nrows=10000)
        train=pd.read_csv(readstring,index_col=0,header=0)
        train=train.reset_index(drop=True)
        train2=train.copy(deep=True)

        #train.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1,inplace=True)
        train.drop(['ts_code','trade_date'],axis=1,inplace=True)


        lgb_model = joblib.load(modelpath)

        dsadwd=lgb_model.feature_importances_

        pred_test = lgb_model.predict(train,num_iteration=lgb_model.best_iteration_)

        data1 = pd.DataFrame(pred_test)

        data1.to_csv(outputname)

        data1['mix']=0
        #multlist=[-10,-3,-2,-1,0,0,1,2,3,10]
        multlist=[-12,-6,-3,-2,-1,1,2,3,6,12]

        #[-12,-8,-3,-2,-1,1,2,3,10,18]
        #[-7,-4,-3,-2,-1,1,2,3,4,12]

        for i in range(10):
            buffer=data1[i]*multlist[i]
            data1['mix']=data1['mix']+buffer

        train2=train2.join(data1)
    
        print(train2)
        
        train2.to_csv(outputname)

        dawd=1

class LGBmodel_notrank(BaseModel):


    def core_predict(self,train,modelpath,savepath):
        """Subclass must implement this."""

        new_train_times=4

        train=train.reset_index(drop=True)
        train2=train.copy(deep=True)

        y_train = np.array(train['tomorrow_chg'])
        train.drop(['tomorrow_chg','ts_code','trade_date'],axis=1,inplace=True)

        for counter in range(new_train_times):
            modelpath_new=os.path.splitext(modelpath)[0]+str(counter)+".pkl"

            lgb_model = joblib.load(modelpath_new)

            dsadwd=lgb_model.feature_importances_
            print(dsadwd)
            pred_test = lgb_model.predict_proba(train)

            data1 = pd.DataFrame(pred_test)

            data1['mix']=0
            #multlist=[-12,-5,-3,-2,-1.5,-1,-0.75,-0.5,-0.25,0,0,0.25,0.5,0.75,1,1.5,2,3,5,12]
            #multlist=[-10,-3,-2,-1,0,0,1,2,3,10]
            multlist=[-8,-8,-3,-2,-1,1,2,3,7,12]
            #multlist=[-8,-8,0,7,12]

            for i in range(10):
                buffer=data1[i]*multlist[i]
                data1['mix']=data1['mix']+buffer

            train3=train2.join(data1)
    
            print(train3)

            savepath_new=os.path.splitext(savepath)[0]+str(counter)+".csv"
            train3.to_csv(savepath_new)

        return 2


    def core_train(self,train,savepath):

        #readstring='ztrain'+year+'.csv'
        
        #readstring=path+'.csv'

        ##train=pd.read_csv(readstring,index_col=0,header=0,nrows=10000)
        #train=pd.read_csv(readstring,index_col=0,header=0)
        train=train.reset_index(drop=True)

        y_train = np.array(train['tomorrow_chg'])
        train.drop(['tomorrow_chg','ts_code','trade_date'],axis=1,inplace=True)

        #x=train.shape[1]
        #y=train.shape[0]
        #Random01=np.random.rand(y, x)
        #Random01[Random01<0.1]=-20
        #Random01[Random01>0]=0
        ##print(Random01)

        ##print(train)
        #train=train+Random01
        #train[Random01<0]=3
        #print(train)
        train_ids = train.index.tolist()

        splitno=int(len(train_ids)*0.70)
        splitno10=int(len(train_ids)*0.15)
        splitno11=int(len(train_ids)*0.85)
        splitno20=int(len(train_ids)*0.35)
        splitno21=int(len(train_ids)*0.65)

        train_index_list=[]
        test_index_list=[]
        #1
        train_index_list.append(train_ids[:splitno])
        test_index_list.append(train_ids[(splitno+10000):])
        #2
        train_index_list.append(train_ids[(len(train_ids)-splitno):])
        test_index_list.append(train_ids[:(len(train_ids)-splitno-10000)])
        #3
        train_index_list.append(train_ids[splitno10:splitno11])
        buffer=train_ids[:(splitno10-10000)]
        buffer.extend(train_ids[(splitno11+10000):])
        test_index_list.append(buffer)
        #4
        buffer=train_ids[:(splitno20)]
        buffer.extend(train_ids[(splitno21):])
        train_index_list.append(buffer)
        test_index_list.append(train_ids[(splitno20+10000):(splitno21-10000)])

        train=train.values

        new_train_times=4

        for counter in range(new_train_times):
       
            X_fit, X_val = train[train_index_list[counter]],train[test_index_list[counter]]
            y_fit, y_val = y_train[train_index_list[counter]], y_train[test_index_list[counter]]

            lgb_model = lgb.LGBMClassifier(max_depth=-1,
                                            n_estimators=220,
                                            learning_rate=0.05,
                                            num_leaves=2**8-1,
                                            colsample_bytree=0.6,
                                            objective='multiclass', 
                                            num_class=10,
                                            n_jobs=-1)
                                   

            #lgb_model.fit(X_fit, y_fit, eval_metric='multi_error',
            #                eval_set=[(X_val, y_val)], 
            #                verbose=100, early_stopping_rounds=1000)
            lgb_model.fit(X_fit, y_fit, eval_metric='multi_error',
                            eval_set=[(X_val, y_val)], 
                            verbose=100, early_stopping_rounds=None)
     
            savepath_new=os.path.splitext(savepath)[0]+str(counter)+".pkl"
            joblib.dump(lgb_model,savepath_new)           




        #X_train,X_test,y_train,y_test=train_test_split(iris.data,iris.target,test_size=0.3)        

        return 2
        #return super().train(dataset)

    def real_lgb_predict(self,modelpath,outputname):
        readstring='today_train.csv'

        #train=pd.read_csv(readstring,index_col=0,header=0,nrows=10000)
        train=pd.read_csv(readstring,index_col=0,header=0)
        train=train.reset_index(drop=True)
        train2=train.copy(deep=True)

        #train.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1,inplace=True)
        train.drop(['ts_code','trade_date'],axis=1,inplace=True)


        lgb_model = joblib.load(modelpath)

        dsadwd=lgb_model.feature_importances_

        pred_test = lgb_model.predict_proba(train)

        data1 = pd.DataFrame(pred_test)

        data1['mix']=0
        #multlist=[-10,-3,-2,-1,0,0,1,2,3,10]
        multlist=[-12,-8,-3,-2,-1,1,2,3,10,18]
        #[-12,-8,-3,-2,-1,1,2,3,10,18]
        #[-7,-4,-3,-2,-1,1,2,3,4,12]

        for i in range(10):
            buffer=data1[i]*multlist[i]
            data1['mix']=data1['mix']+buffer

        train2=train2.join(data1)
    
        print(train2)
        
        train2.to_csv(outputname)

        dawd=1