#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载行业分类相关信息
"""
import sys
from WindPy import w
from datetime import datetime
from kquant_data.wind.tdays import read_tdays
from kquant_data.wind_resume.wset import download_sector, download_sectors
from kquant_data.config import __CONFIG_H5_STK_SECTOR_DIR__, __CONFIG_TDAYS_SSE_FILE__


# 解决Python 3.6的pandas不支持中文路径的问题
print(sys.getfilesystemencoding())  # 查看修改前的
try:
    sys._enablelegacywindowsfsencoding()  # 修改
    print(sys.getfilesystemencoding())  # 查看修改后的
except:
    pass

if __name__ == '__main__':
    w.start()

    date_str = datetime.today().strftime('%Y-%m-%d')

    trading_days = read_tdays(__CONFIG_TDAYS_SSE_FILE__)
    trading_days = trading_days['2004-06-01':date_str]

    # 按频率来看数据是稀疏的，但需要每天下载一次
    if False:
        download_sectors(w, trading_days, sector_name="中信证券一级行业指数", root_path=__CONFIG_H5_STK_SECTOR_DIR__)
    if True:
        download_sector(w, trading_days, sector_name="风险警示股票", root_path=__CONFIG_H5_STK_SECTOR_DIR__)
