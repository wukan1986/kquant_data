#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载指数权重
万得中的中证指数每个月更新一次，用的是上月月末数据
如要实时更新，需要联系客户经理
可能有部分数据出现nan的情况，如纳入成份股时
"""
import os
from WindPy import w
from datetime import datetime
from kquant_data.wind.tdays import read_tdays
from kquant_data.xio.csv import read_data_dataframe
from kquant_data.wind_resume.wset import download_index_weight

from kquant_data.config import __CONFIG_H5_STK_FACTOR_DIR__, __CONFIG_H5_STK_WEIGHT_DIR__, __CONFIG_TDAYS_SSE_FILE__

if __name__ == '__main__':
    # w.start()
    date_str = datetime.today().strftime('%Y-%m-%d')

    trading_days = read_tdays(__CONFIG_TDAYS_SSE_FILE__)
    # 4月8号是指数的发布日期
    trading_days = trading_days['2005-04-08':date_str]

    # 下载多天数据,以另一数据做为标准来下载
    # 比如交易数据是10月8号，那就得取10月7号，然后再平移到8号，如果7号没有数据那就得9月30号
    path = os.path.join(__CONFIG_H5_STK_FACTOR_DIR__, 'test.csv')
    date_index = read_data_dataframe(path)

    # dates = list(date_index.index)
    download_index_weight(w, trading_days, "000300.SH", __CONFIG_H5_STK_WEIGHT_DIR__)
