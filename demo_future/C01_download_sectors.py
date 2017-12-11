#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载版块相关信息
"""
from WindPy import w
from datetime import datetime, timedelta
from kquant_data.wind.tdays import read_tdays
from kquant_data.wind_resume.wset import download_sector
from kquant_data.config import __CONFIG_H5_FUT_SECTOR_DIR__, __CONFIG_TDAYS_SHFE_FILE__

if __name__ == '__main__':
    w.start()

    trading_days = read_tdays(__CONFIG_TDAYS_SHFE_FILE__)

    # 移动到下一个交易日
    date_str = (datetime.today() + timedelta(days=0)).strftime('%Y-%m-%d')
    new_trading_days = trading_days[date_str:]
    date_str = (new_trading_days.iloc[1, 0]).strftime('%Y-%m-%d')

    new_trading_days = trading_days['1999-01-04':date_str]
    download_sector(w, new_trading_days, sector_name="大商所全部品种", root_path=__CONFIG_H5_FUT_SECTOR_DIR__)
    new_trading_days = trading_days['1995-04-17':date_str]
    download_sector(w, new_trading_days, sector_name="上期所全部品种", root_path=__CONFIG_H5_FUT_SECTOR_DIR__)
    new_trading_days = trading_days['1999-01-04':date_str]
    download_sector(w, new_trading_days, sector_name="郑商所全部品种", root_path=__CONFIG_H5_FUT_SECTOR_DIR__)
    new_trading_days = trading_days['2010-04-19':date_str]
    download_sector(w, new_trading_days, sector_name="中金所全部品种", root_path=__CONFIG_H5_FUT_SECTOR_DIR__)



