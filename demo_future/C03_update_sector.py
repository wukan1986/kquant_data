#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
上期所下有原油交易中心的合约sc
sc在上市之前仿真了很久，导致下载的文件中有大量的仿真合约，需要清理
"""
import os
import sys
import pandas as pd
from kquant_data.config import __CONFIG_H5_FUT_SECTOR_DIR__
from kquant_data.wind.wset import write_constituent, read_constituent

# 解决Python 3.6的pandas不支持中文路径的问题
print(sys.getfilesystemencoding())  # 查看修改前的
try:
    sys._enablelegacywindowsfsencoding()  # 修改
    print(sys.getfilesystemencoding())  # 查看修改后的
except:
    pass

if __name__ == '__main__':
    path = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, "上期所全部品种")

    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            if filename <= '2017-12-26.csv':
                continue
            # sc2018年3月26号上市
            if filename >= '2018-03-26.csv':
                continue
            new_path = os.path.join(dirpath, filename)

            df_csv = read_constituent(new_path)
            sym_ex = df_csv['wind_code'].str.split('.')
            sym_ex = list(sym_ex)
            sym_ex_df = pd.DataFrame(sym_ex)
            sym_ex_df.columns = ['InstrumentID', 'exchange']
            df = pd.concat([df_csv, sym_ex_df], axis=1)
            df = df[df['exchange'] != 'INE']
            df = df[['wind_code']]

            if len(df) < len(df_csv):
                write_constituent(new_path, df)
                print(new_path)

            debug = 1

            # df = pd.read_csv(new_path, encoding='gbk', parse_dates=True)

    # w.start()
    #
    # trading_days = read_tdays(__CONFIG_TDAYS_SHFE_FILE__)
    #
    # # 移动到下一个交易日
    # date_str = (datetime.today() + timedelta(days=0)).strftime('%Y-%m-%d')
    # new_trading_days = trading_days[date_str:]
    # date_str = (new_trading_days.iloc[1, 0]).strftime('%Y-%m-%d')
    #
    # new_trading_days = trading_days['1999-01-04':date_str]
    # download_sector(w, new_trading_days, sector_name="大商所全部品种", root_path=__CONFIG_H5_FUT_SECTOR_DIR__)
    # new_trading_days = trading_days['1995-04-17':date_str]
    # download_sector(w, new_trading_days, sector_name="上期所全部品种", root_path=__CONFIG_H5_FUT_SECTOR_DIR__)
    # new_trading_days = trading_days['1999-01-04':date_str]
    # download_sector(w, new_trading_days, sector_name="郑商所全部品种", root_path=__CONFIG_H5_FUT_SECTOR_DIR__)
    # new_trading_days = trading_days['2010-04-19':date_str]
    # download_sector(w, new_trading_days, sector_name="中金所全部品种", root_path=__CONFIG_H5_FUT_SECTOR_DIR__)
