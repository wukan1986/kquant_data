#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
同一品种的下载流程
"""
from WindPy import w
import os
import numpy as np
from datetime import datetime,timedelta
import pandas as pd
from kquant_data.wind.tdays import read_tdays
from kquant_data.config import __CONFIG_TDAYS_SSE_FILE__
from kquant_data.wind_resume.wset import resume_download_futureoir
from kquant_data.future.symbol import get_actvie_products_wind

if __name__ == '__main__':
    # 很多品种的2016-04-21没有数据，如TA.CZC\ZC.CZC\SR.CZC\OI.CZC\MA.CZC\RM.CZC\FG.CZC\CF.CZC\WH.CZC，都郑商所

    # w = None
    w.start()

    # 最新的一天由于没有数据，会导致参与下载，如果中间某一天缺失，会导致大量下载数据，需要排除
    date_str = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    # date_str = datetime.today().strftime('%Y-%m-%d')
    # IF一类的起始时间是从2010年开始，不能再从2009年开始遍历了

    trading_days = read_tdays(__CONFIG_TDAYS_SSE_FILE__)
    # trading_days = trading_days['2016-01-01':date_str]
    trading_days = trading_days['2012-06-01':'2014-12-31']

    # 下载后存下
    windcodes = get_actvie_products_wind()
    root_path = r'D:\DATA_FUT\futureoir'
    for windcode in windcodes:
        print('处理', windcode)
        resume_download_futureoir(w, trading_days, root_path, windcode, adjust_trading_days=False)
