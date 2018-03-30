#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载合约日线
由于数据量不是很大，可以全下
"""
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
from WindPy import w

from kquant_data.config import __CONFIG_TDAYS_SHFE_FILE__, __CONFIG_H5_FUT_SECTOR_DIR__
from kquant_data.utils.symbol import split_alpha_number
from kquant_data.utils.xdatetime import yyyyMMdd_2_datetime
from kquant_data.wind.tdays import read_tdays
from kquant_data.wind.wsd import download_daily_ohlcv
from kquant_data.wind.wset import read_constituent
from kquant_data.xio.csv import read_datetime_dataframe

# 解决Python 3.6的pandas不支持中文路径的问题
print(sys.getfilesystemencoding())  # 查看修改前的
try:
    sys._enablelegacywindowsfsencoding()  # 修改
    print(sys.getfilesystemencoding())  # 查看修改后的
except:
    pass


def read_constituent_at_date(dirpath, date):
    date_csv = '%s.csv' % date.strftime('%Y-%m-%d')

    path = os.path.join(dirpath, date_csv)
    df = read_constituent(path)
    if df is None:
        # 对于指数数据，长期在线
        date_csv = '0.csv'
        path = os.path.join(dirpath, date_csv)
        df = read_constituent(path)
    return df


def merge_constituent_date(constituent, ipo_last_trade, first_last):
    constituent_dt = constituent.merge(ipo_last_trade, how='left', left_on='wind_code', right_on='wind_code')
    constituent_dt = constituent_dt.merge(first_last, how='left', left_on='wind_code', right_index=True)
    constituent_dt['start'] = constituent_dt.apply(lambda row: max(row['ipo_date'], row['last']), axis=1)
    constituent_dt['end'] = constituent_dt.apply(lambda row: min(row['lasttrade_date'], datetime.today()), axis=1)
    # 没有结束时间的，调整成当前时间
    constituent_dt['end'] = constituent_dt['end'].fillna(datetime.today())

    if True:
        # 不下载仿真合约
        constituent_dt = constituent_dt[~constituent_dt['sec_name'].str.contains('仿真')]

    return constituent_dt


def download_constituent_daily(w, dirpath, date, ipo_last_trade, first_last, wind_code_set, fields):
    constituent = read_constituent_at_date(dirpath, date)
    if constituent is None:
        # 没有对应的板块，因当是与上次一样导致
        # 没关系，上次数据应当已经下载过了
        return wind_code_set
    constituent_dt = merge_constituent_date(constituent, ipo_last_trade, first_last)

    for i in range(constituent_dt.shape[0]):
        row = constituent_dt.iloc[i]
        wind_code = row['wind_code']
        # 当前会话，不重复下载
        if wind_code in wind_code_set:
            continue
        wind_code_set.add(wind_code)
        # 时间已经到期了，不重复下载
        # 这里使用日期，不考虑时间
        if row['start'].date() == row['end'].date():
            continue

        product, num = split_alpha_number(wind_code)
        path_dir = os.path.join(root_path, product)
        if not os.path.exists(path_dir):
            os.mkdir(path_dir)
        path_csv = os.path.join(path_dir, '%s.csv' % wind_code)
        try:
            df_old = pd.read_csv(path_csv, index_col=0, parse_dates=True)
        except:
            df_old = None
        print(row)
        df_new, wind_code = download_daily_ohlcv(w, wind_code, row['start'], row['end'], fields)
        df = pd.concat([df_old, df_new])
        df = df[~df.index.duplicated(keep='last')]
        print(path_csv)
        df.to_csv(path_csv)
    return wind_code_set


if __name__ == '__main__':
    w.start()

    # 注意，数据中有可能出现0
    path_ipo_last_trade = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, 'ipo_last_trade_trading.csv')
    ipo_last_trade = pd.read_csv(path_ipo_last_trade)
    ipo_last_trade['ipo_date'] = ipo_last_trade['ipo_date'].apply(lambda x: yyyyMMdd_2_datetime(x))
    ipo_last_trade['lasttrade_date'] = ipo_last_trade['lasttrade_date'].apply(lambda x: yyyyMMdd_2_datetime(x))

    root_path = r'D:\DATA_FUT\86400'
    path_first_last = os.path.join(root_path, 'first_last.csv')
    first_last = read_datetime_dataframe(path_first_last)

    date_str = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    # 设置至少30天比较靠谱
    start_str = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')

    trading_days = read_tdays(__CONFIG_TDAYS_SHFE_FILE__)
    trading_days = trading_days[start_str:date_str]

    wind_code_set = set()

    if True:
        fields = 'open,high,low,close,volume'
        dirpath = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, '南华期货商品指数')
        for i in range(len(trading_days)):
            wind_code_set = download_constituent_daily(w, dirpath, trading_days['date'][i], ipo_last_trade, first_last,
                                                       wind_code_set, fields)

    if False:
        fields = 'open,high,low,close,volume,amt,oi,settle'
        dirpath = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, '中金所全部品种')
        for i in range(len(trading_days)):
            wind_code_set = download_constituent_daily(w, dirpath, trading_days['date'][i], ipo_last_trade, first_last,
                                                       wind_code_set, fields)

        dirpath = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, '上期所全部品种')
        for i in range(len(trading_days)):
            wind_code_set = download_constituent_daily(w, dirpath, trading_days['date'][i], ipo_last_trade, first_last,
                                                       wind_code_set, fields)

        dirpath = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, '大商所全部品种')
        for i in range(len(trading_days)):
            wind_code_set = download_constituent_daily(w, dirpath, trading_days['date'][i], ipo_last_trade, first_last,
                                                       wind_code_set, fields)

        dirpath = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, '郑商所全部品种')
        for i in range(len(trading_days)):
            wind_code_set = download_constituent_daily(w, dirpath, trading_days['date'][i], ipo_last_trade, first_last,
                                                       wind_code_set, fields)
