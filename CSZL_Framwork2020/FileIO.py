#coding=utf-8

import os
import shutil



class FileIO(object):
    """description of class"""

    def mkdir(path):
        """生成文件夹路径"""
 
        # 去除首位空格
        path=path.strip()
        # 去除尾部 \ 符号
        path=path.rstrip("\\")
 
        # 判断路径是否存在
        # 存在     True
        # 不存在   False
        isExists=os.path.exists(path)
 
        # 判断结果
        if not isExists:

            os.makedirs(path) 
 
            print (path+' 创建成功')
            return True
        else:
            # 如果目录存在则不创建，并提示目录已存在
            print (path+' 目录已存在')
            return False

    def copyfile(srcfile,dstfile):
        """复制文件"""

        if not os.path.isfile(srcfile):
            print ("%s not exist!"%(srcfile))
        else:
            fpath,fname=os.path.split(dstfile)    #分离文件名和路径
            if not os.path.exists(fpath):
                os.makedirs(fpath)                #创建路径
            shutil.copyfile(srcfile,dstfile)      #复制文件
            print ("copy %s -> %s"%( srcfile,dstfile))