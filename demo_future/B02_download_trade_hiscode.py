#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载主力和次主力交易代码
C#部分会在使用时对郑商所补成4位再用，而csv文件中的3位可以先不动
"""
from WindPy import w
import os
from datetime import datetime, timedelta
from kquant_data.config import __CONFIG_H5_FUT_FACTOR_DIR__
from kquant_data.xio.csv import read_datetime_dataframe
from kquant_data.wind.tdays import read_tdays
from kquant_data.wind_resume.wsd import resume_download_daily_to_many_files, \
    resume_download_financial_report_data_daily_latest

from kquant_data.config import __CONFIG_TDAYS_SHFE_FILE__

if __name__ == '__main__':
    w.start()

    trading_days = read_tdays(__CONFIG_TDAYS_SHFE_FILE__)

    # 读取合约上市日期，提前做好准备
    path = os.path.join(__CONFIG_H5_FUT_FACTOR_DIR__, 'contract_issuedate.csv')
    contract_issuedate = read_datetime_dataframe(path)

    wind_codes = list(contract_issuedate.columns)

    # 下载最新一天的数据
    field = 'trade_hiscode'

    # 下载最新一天，并且合并历史
    date_str = (datetime.today() + timedelta(days=0)).strftime('%Y-%m-%d')
    resume_download_financial_report_data_daily_latest(w, wind_codes, trading_days, __CONFIG_H5_FUT_FACTOR_DIR__, field,
                                                       date_str)

    # 使用下一个交易日
    new_trading_days = trading_days[date_str:]
    date_str = (new_trading_days.iloc[1, 0]).strftime('%Y-%m-%d')
    resume_download_financial_report_data_daily_latest(w, wind_codes, trading_days, __CONFIG_H5_FUT_FACTOR_DIR__, field,
                                                       date_str)

    # 补数据
    resume_download_daily_to_many_files(w, trading_days, contract_issuedate, __CONFIG_H5_FUT_FACTOR_DIR__, field)

    # 上次的合约更名后，这一次再补数时，会认为是不同的合约，过于麻烦，所以郑商所3位变4位还是由C#程序来实现得了
