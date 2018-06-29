#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载合约信息
"""
from WindPy import w
import sys
import os
import pandas as pd
from kquant_data.wind.wss import download_ipo_last_trade_trading
from kquant_data.xio.csv import write_data_dataframe, read_data_dataframe
from kquant_data.config import __CONFIG_H5_FUT_SECTOR_DIR__
from kquant_data.wind.wset import read_constituent


# 解决Python 3.6的pandas不支持中文路径的问题
print(sys.getfilesystemencoding())  # 查看修改前的
try:
    sys._enablelegacywindowsfsencoding()  # 修改
    print(sys.getfilesystemencoding())  # 查看修改后的
except:
    pass

def process_dir2file(w, mydir, myfile):
    df = read_data_dataframe(myfile)
    all_set = set(df.index)
    for dirpath, dirnames, filenames in os.walk(mydir):
        for filename in filenames:
            # 这个日期需要记得修改
            if filename < "2017-01-01.csv":
                continue

            filepath = os.path.join(dirpath, filename)

            df1 = read_constituent(filepath)
            # print(filepath)
            if df1 is None:
                continue
            if df1.empty:
                continue
            curr_set = set(df1['wind_code'])
            diff_set = curr_set - all_set
            if len(diff_set) == 0:
                continue

            print(filepath)
            df2 = download_ipo_last_trade_trading(w, list(diff_set))
            df = pd.concat([df, df2])
            all_set = set(df.index)
            # 出于安全考虑，还是每次都保存
            write_data_dataframe(myfile, df)

    df['wind_code'] = df.index
    df.sort_values(by=['ipo_date', 'wind_code'], inplace=True)
    del df['wind_code']
    write_data_dataframe(myfile, df)


if __name__ == '__main__':
    w.start()

    # 先读取数据，合并，找不同，然后下单
    outputFile = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, 'ipo_last_trade_trading.csv')

    path = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, '郑商所全部品种')
    process_dir2file(w, path, outputFile)

    path = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, '中金所全部品种')
    process_dir2file(w, path, outputFile)

    path = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, '上期所全部品种')
    process_dir2file(w, path, outputFile)

    path = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, '大商所全部品种')
    process_dir2file(w, path, outputFile)
