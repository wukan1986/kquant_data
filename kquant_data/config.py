#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
配置信息

股票数据要能正常运行，需要准备工作如下
1. 安装通达信（日线数据来源），然后配置实际的__CONFIG_TDX_STK_DIR__
2. 安装大智慧（除权除息来源），确保路径__CONFIG_DZH_PWR_FILE__的目录已经创建。也可不安装大智慧直接建立对应的目录
3. 根据以下的配置文件，在D盘创建相应的目录
"""
import os

"""
股票
"""
# 通达信目录
__CONFIG_TDX_STK_DIR__ = r'D:\new_hbzq'
# 大智慧权息文件
__CONFIG_DZH_PWR_FILE__ = r"D:\dzh2\Download\PWR\full.PWR"
# 输出的股票行情目录
__CONFIG_H5_STK_DIR__ = r'D:\DATA_STK'


# 股票的除权除息数据
__CONFIG_H5_STK_DIVIDEND_DIR__ = os.path.join(__CONFIG_H5_STK_DIR__, 'dividend')
# 股票行业信息
__CONFIG_H5_STK_SECTOR_DIR__ = os.path.join(__CONFIG_H5_STK_DIR__, 'sectorconstituent')
# 指数权重信息
__CONFIG_H5_STK_WEIGHT_DIR__ = os.path.join(__CONFIG_H5_STK_DIR__, 'indexconstituent')
# 股票因子信息
__CONFIG_H5_STK_FACTOR_DIR__ = os.path.join(__CONFIG_H5_STK_DIR__, 'factor')
# 交易日数据，请每年末更新一下第二年的交易日信息
__CONFIG_TDAYS_SSE_FILE__ = os.path.join(__CONFIG_H5_STK_DIR__, 'tdays', 'SSE.csv')

"""
期货
"""
# 期货行情数据
__CONFIG_H5_FUT_DIR__ = r'D:\DATA_FUT'

__CONFIG_H5_FUT_FACTOR_DIR__ = os.path.join(__CONFIG_H5_FUT_DIR__, 'factor')

__CONFIG_H5_FUT_SECTOR_DIR__ = os.path.join(__CONFIG_H5_FUT_DIR__, 'sectorconstituent')

__CONFIG_H5_FUT_DATA_DIR__ = os.path.join(__CONFIG_H5_FUT_DIR__, 'data')

__CONFIG_TDAYS_SHFE_FILE__ = os.path.join(__CONFIG_H5_FUT_DIR__, 'tdays', 'SHFE.csv')

__CONFIG_H5_FUT_MARKET_DATA_DIR__ = r'D:\DATA_FUT_HDF5\Data_P2'



# 为了将自定义库引入进来，注意这里要改
# sys.path.append(r'D:\Python\Kan')
# sys.path
# 使用这种方法更方便
# D:\Users\Kan\Anaconda3\Lib\site-packages\kquant.pth
