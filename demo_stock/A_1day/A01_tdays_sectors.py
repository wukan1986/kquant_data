#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
执行次数很早的算法
比如下载行业分类列表，下载
"""
import sys
from WindPy import w
from kquant_data.config import __CONFIG_TDAYS_SSE_FILE__, __CONFIG_H5_STK_SECTOR_DIR__

from kquant_data.wind_resume.wset import download_sectors_list
from kquant_data.wind_resume.tdays import resume_download_tdays


# 解决Python 3.6的pandas不支持中文路径的问题
print(sys.getfilesystemencoding())  # 查看修改前的
try:
    sys._enablelegacywindowsfsencoding()  # 修改
    print(sys.getfilesystemencoding())  # 查看修改后的
except:
    pass


if __name__ == '__main__':
    w.start()
    # 因为只用下载一次，所以都用False先关闭

    # 下载行业分类列表，只用下载一次即可
    if False:
        download_sectors_list(w,
                              sector_name="中信证券一级行业指数",
                              root_path=__CONFIG_H5_STK_SECTOR_DIR__)

    # 下载交易日，在每年的最后几周下即即可，需手工修改
    if True:
        resume_download_tdays(w,
                              enddate='2018-12-28',
                              path=__CONFIG_TDAYS_SSE_FILE__)
