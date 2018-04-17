#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载合约乘数和执行价
"""
from WindPy import w
import os
import pandas as pd
from kquant_data.config import __CONFIG_H5_OPT_DIR__, __CONFIG_H5_STK_DIR__

from kquant_data.option.info import get_opt_info
from kquant_data.utils.xdatetime import yyyyMMddHHmm_2_datetime
from kquant_data.xio.csv import write_data_dataframe
from kquant_data.wind.wset import download_optionchain


def get_sector_at(df_info, date):
    idx = (df_info['listed_date'] <= date) & (df_info['expire_date'] >= date)
    df_info2 = df_info[idx]
    if df_info2.empty:
        return None
    # print(df_info2)
    return df_info2


def download_exe_price(w, sector, date):
    if sector is None:
        return

    path = os.path.join(__CONFIG_H5_OPT_DIR__, 'optionchain', '510050.SH', '%s.csv' % date)
    if os.path.exists(path):
        return
    print('准备下载数据')
    df = download_optionchain(w, date, '510050.SH')
    write_data_dataframe(path, df)


if __name__ == '__main__':
    w.start()

    # 获取期权基础信息文件
    df_info = get_opt_info('510050.SH.csv')

    # 得到除权日
    path = os.path.join(__CONFIG_H5_STK_DIR__, 'dividend', 'sh510050.h5')
    div = pd.read_hdf(path)

    for i in range(div.shape[0]):
        date_right = yyyyMMddHHmm_2_datetime(div.ix[i, 'time'])
        date_left = date_right - pd.Timedelta('1D')
        print('=' * 60)
        print(date_left, date_right)
        print('=' * 60)
        print(date_left)
        date = date_left.strftime('%Y-%m-%d')
        sector_left = get_sector_at(df_info, date)
        # TODO:下次得处理已经存在就没有必要下载的问题
        download_exe_price(w, sector_left, date)

        print('=' * 60)
        print(date_right)
        date = date_right.strftime('%Y-%m-%d')
        sector_right = get_sector_at(df_info, date)
        download_exe_price(w, sector_right, date)
