#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载版块相关信息
"""
from WindPy import w
from datetime import datetime
import os
import pandas as pd
from kquant_data.wind.tdays import read_tdays
from kquant_data.future.symbol import CZCE_3to4
from kquant_data.wind_resume.wset import download_sector, download_sectors, read_constituent, write_constituent
from kquant_data.config import __CONFIG_H5_FUT_SECTOR_DIR__, __CONFIG_TDAYS_SSE_FILE__


def process_CZCE():
    # 郑商所的合约，对于已经退市的合约需要将3位变成4位
    # 最后一个文件肯定是最新的，它是当前正在交易的合约，应当全是3位，然后遍历其它文件没有出现的就可以改名
    path = r'D:\DATA_FUT\sectorconstituent\郑商所全部品种'
    filename_list = []
    for dirpath, dirnames, filenames in os.walk(path, topdown=False):
        for filename in filenames:
            # print(filename)
            filename_list.append(filename)
    last = filename_list[-1]
    last_path = os.path.join(path, last)
    last_df = read_constituent(last_path)
    last_set = set(last_df['wind_code'])
    for filename in reversed(filename_list):
        curr_path = os.path.join(path, filename)
        print(curr_path)
        curr_df = read_constituent(curr_path)
        curr_set = set(curr_df['wind_code'])
        diff_set = curr_set - last_set
        if len(diff_set) == 0:
            print("两天之间合约是一样的，跳过")
            continue

        mod_diff_set = set(map(CZCE_3to4, diff_set))
        check_set = diff_set - mod_diff_set

        if len(check_set) == 0:
            print("调整后还是一样的，不保存")
            continue

        new_set = mod_diff_set | (curr_set & last_set)
        new_df = pd.DataFrame(list(new_set), columns=['wind_code'])
        new_df.sort_values(by=['wind_code'], inplace=True)
        write_constituent(curr_path, new_df)
        print("有变化")

if __name__ == '__main__':
    w.start()

    date_str = datetime.today().strftime('%Y-%m-%d')

    trading_days = read_tdays(__CONFIG_TDAYS_SSE_FILE__)
    # 由于这个日期比较特殊，会导致每次都下单
    trading_days = trading_days['2017-01-01':date_str]

    # process_CZCE()

    download_sector(w, trading_days, sector_name="郑商所全部品种", root_path=__CONFIG_H5_FUT_SECTOR_DIR__)


    # 按频率来看数据是稀疏的，但需要每天下载一次
    # if True:
    # download_sector(w, trading_days, sector_name="中金所全部品种", root_path=__CONFIG_H5_FUT_SECTOR_DIR__)
    # download_sector(w, trading_days, sector_name="上期所全部品种", root_path=__CONFIG_H5_FUT_SECTOR_DIR__)
    # download_sector(w, trading_days, sector_name="大商所全部品种", root_path=__CONFIG_H5_FUT_SECTOR_DIR__)
