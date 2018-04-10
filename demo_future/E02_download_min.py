#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载合约分钟
由于数据量很大，可以只下载一些关键的
比如
主力、次主力
近月、远月

一定要通过update_first_last.py更新数据的下载范围
否则会全下，还会覆盖前面的数据
"""
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
from WindPy import w
from demo_future.E01_download_daily import read_constituent_at_date, merge_constituent_date
from kquant_data.config import __CONFIG_TDAYS_SHFE_FILE__, __CONFIG_H5_FUT_SECTOR_DIR__
from kquant_data.utils.symbol import split_alpha_number
from kquant_data.utils.xdatetime import yyyyMMdd_2_datetime, datetime_2_yyyyMMddHHmm
from kquant_data.wind.tdays import read_tdays
from kquant_data.wind.wsi import download_min_ohlcv
from kquant_data.xio.csv import read_datetime_dataframe

# 解决Python 3.6的pandas不支持中文路径的问题
print(sys.getfilesystemencoding())  # 查看修改前的
try:
    sys._enablelegacywindowsfsencoding()  # 修改
    print(sys.getfilesystemencoding())  # 查看修改后的
except:
    pass


def download_constituent_min(w, dirpath, date, ipo_last_trade, first_last, wind_code_set, trading_days):
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
        # 这里考虑时间
        if datetime_2_yyyyMMddHHmm(row['start']) == datetime_2_yyyyMMddHHmm(row['end']):
            continue

        product, num = split_alpha_number(wind_code)
        path_dir = os.path.join(root_path, product)
        if not os.path.exists(path_dir):
            os.mkdir(path_dir)
        path_csv = os.path.join(path_dir, '%s.csv' % wind_code)

        # 从开始到结束，可能跨度太长，特别是新下数据，可能超过一年，所以决定按月下载，这样更快
        # 保存时如果多次保存，然后放在一个文件中，会有问题
        trading_days['idx'] = range(len(trading_days))

        start = row['start']
        end = row['end']
        trading_days_idx = trading_days[start._date_repr:end._date_repr]['idx']
        rng = list(range(trading_days_idx[0], trading_days_idx[-1], 30))
        # 右闭
        rng.insert(len(rng), trading_days_idx[-1])
        for idx, r in enumerate(rng):
            if idx == 0:
                continue
            start_ = trading_days.iloc[rng[idx - 1]]['date']
            end_ = trading_days.iloc[r]['date']
            if idx == 1:
                # 第一个位置比较特殊，要取到前一个交易日的晚上
                start_ = trading_days.iloc[rng[idx - 1] - 1]['date']

            start_ += pd.Timedelta('20H')  # 会遇到不是交易日的问题
            end_ += pd.Timedelta('16H')
            print(start_, end_)
            try:
                df_old = pd.read_csv(path_csv, index_col=0, parse_dates=True)
                # 合并前先删除空数据
                df_old.dropna(axis=0, how='all', thresh=3, inplace=True)
            except:
                df_old = None
            print(row)
            df_new, wind_code = download_min_ohlcv(w, wind_code, start, end)
            df = pd.concat([df_old, df_new])
            df = df[~df.index.duplicated(keep='last')]
            print(path_csv)
            df.to_csv(path_csv)
        # 只做一个测试
        # break

    return wind_code_set


if __name__ == '__main__':
    # 数据太多，是否减少数据下载更合适？比如只下主力，次主力，最近月，最远月
    # 也可选择全下

    # 分钟数据是按天分割，还是放在一个文件中呢？
    # 下载是从哪下到哪呢？

    w.start()

    # 注意，数据中有可能出现0
    path_ipo_last_trade = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, 'ipo_last_trade_trading.csv')
    ipo_last_trade = pd.read_csv(path_ipo_last_trade)
    ipo_last_trade['ipo_date'] = ipo_last_trade['ipo_date'].apply(lambda x: yyyyMMdd_2_datetime(x))
    ipo_last_trade['lasttrade_date'] = ipo_last_trade['lasttrade_date'].apply(lambda x: yyyyMMdd_2_datetime(x))

    root_path = r'D:\DATA_FUT\60'
    path_first_last = os.path.join(root_path, 'first_last.csv')
    first_last = read_datetime_dataframe(path_first_last)

    date_str = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    # 设置至少30天比较靠谱
    start_str = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')

    trading_days = read_tdays(__CONFIG_TDAYS_SHFE_FILE__)
    trading_days_slice = trading_days[start_str:date_str]

    wind_code_set = set()

    dirpath = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, '中金所全部品种')
    for i in range(len(trading_days_slice)):
        wind_code_set = download_constituent_min(w, dirpath, trading_days_slice['date'][i], ipo_last_trade, first_last,
                                                 wind_code_set, trading_days)

    dirpath = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, '中金所全部品种')
    for i in range(len(trading_days_slice)):
        wind_code_set = download_constituent_min(w, dirpath, trading_days_slice['date'][i], ipo_last_trade, first_last,
                                                 wind_code_set, trading_days)

    dirpath = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, '上期所全部品种')
    for i in range(len(trading_days_slice)):
        wind_code_set = download_constituent_min(w, dirpath, trading_days_slice['date'][i], ipo_last_trade, first_last,
                                                 wind_code_set, trading_days)

    dirpath = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, '大商所全部品种')
    for i in range(len(trading_days_slice)):
        wind_code_set = download_constituent_min(w, dirpath, trading_days_slice['date'][i], ipo_last_trade, first_last,
                                                 wind_code_set, trading_days)

    dirpath = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, '郑商所全部品种')
    for i in range(len(trading_days_slice)):
        wind_code_set = download_constituent_min(w, dirpath, trading_days_slice['date'][i], ipo_last_trade, first_last,
                                                 wind_code_set, trading_days)
