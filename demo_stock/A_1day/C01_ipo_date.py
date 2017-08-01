#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
执行次数很早的算法
比如下载行业分类列表，下载
"""
import os
import numpy as np
from WindPy import w
from kquant_data.config import __CONFIG_H5_STK_DIR__, __CONFIG_H5_STK_FACTOR_DIR__
from kquant_data.wind_resume.wsd import resume_download_delist_date
from kquant_data.api import all_instruments

if __name__ == '__main__':
    w.start()

    # 加载股票列表，这里需要在每天收盘后导出日线数据才能做
    path = os.path.join(__CONFIG_H5_STK_DIR__, '1day', 'Symbol.csv')
    Symbols = all_instruments(path)
    wind_codes = Symbols['wind_code']

    # 增量下载ipo_date，由于每周都有上市，但因为新上市股票不参加交易，所以看情况进行
    if False:
        resume_download_delist_date(w, wind_codes, __CONFIG_H5_STK_FACTOR_DIR__,
                                    field='ipo_date',
                                    dtype=np.datetime64)
    # 增量下载delist_date，由于退市频率比较低，隔一定时间做一次即可
    if False:
        resume_download_delist_date(w, wind_codes, __CONFIG_H5_STK_FACTOR_DIR__,
                                    field='delist_date',
                                    dtype=np.datetime64)
