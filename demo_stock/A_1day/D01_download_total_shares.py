#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载财报一类的信息
"""
import os
from WindPy import w
from kquant_data.xio.csv import read_datetime_dataframe
from kquant_data.api import all_instruments
from kquant_data.wind.tdays import read_tdays
from kquant_data.wind_resume.wsd import resume_download_daily_to_many_files, \
    resume_download_financial_report_data_daily_latest

from kquant_data.config import __CONFIG_H5_STK_FACTOR_DIR__, __CONFIG_H5_STK_DIR__, __CONFIG_TDAYS_SSE_FILE__

if __name__ == '__main__':
    w.start()

    path = os.path.join(__CONFIG_H5_STK_DIR__, '1day', 'Symbol.csv')
    Symbols = all_instruments(path)
    wind_codes = list(Symbols['wind_code'])

    trading_days = read_tdays(__CONFIG_TDAYS_SSE_FILE__)

    # 读取ipo_date，提前做好准备
    path = os.path.join(__CONFIG_H5_STK_FACTOR_DIR__, 'ipo_date.csv')
    ipo_date = read_datetime_dataframe(path)

    # 下载最新一天的数据
    field = 'total_shares'
    # 下载最新一天，并且合并历史
    resume_download_financial_report_data_daily_latest(w, wind_codes, trading_days, __CONFIG_H5_STK_FACTOR_DIR__, field)
    resume_download_daily_to_many_files(w, trading_days, ipo_date, __CONFIG_H5_STK_FACTOR_DIR__, field)
