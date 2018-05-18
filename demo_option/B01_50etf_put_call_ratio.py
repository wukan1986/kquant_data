#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
兴业证券
利用期权市场进行择时之二：依据期权指标判断市场走势
"""
import os
import pandas as pd

from kquant_data.api import get_price
from kquant_data.option.info import get_opt_info
from kquant_data.config import __CONFIG_H5_OPT_DIR__, __CONFIG_TDX_STK_DIR__


def main2():
    # 获取期权基础信息文件
    df_info = get_opt_info('510050.SH.csv')
    df_info_call = df_info[df_info['call_or_put'] == 'C']
    df_info_put = df_info[df_info['call_or_put'] == 'P']
    df_info_call.reset_index(inplace=True)
    df_info_put.reset_index(inplace=True)

    path = os.path.join(__CONFIG_TDX_STK_DIR__, 'vipdoc', 'ds', 'lday')

    wind_codes = list(set(df_info_call['wind_code']))
    wind_codes.sort()
    df_volume = None
    for wind_code in wind_codes:
        print(wind_code)
        try:
            df_volume_ = get_price(wind_code, path=path, instrument_type='option',
                                   fields=['Volume'])
        except:
            continue
        if df_volume is None:
            df_volume = df_volume_
        else:
            # 将所有数据合并成一个DataFrame太占用内存，读一个计算一个更快
            df_volume = df_volume.add(df_volume_, fill_value=0)
    path_csv = os.path.join(__CONFIG_H5_OPT_DIR__, 'data', 'call.csv')
    df_volume.to_csv(path_csv)

    wind_codes = list(set(df_info_put['wind_code']))
    wind_codes.sort()
    df_volume = None
    for wind_code in wind_codes:
        print(wind_code)
        try:
            df_volume_ = get_price(wind_code, path=path, instrument_type='option',
                                   fields=['Volume'])
        except:
            continue
        if df_volume is None:
            df_volume = df_volume_
        else:
            df_volume = df_volume.add(df_volume_, fill_value=0)
    path_csv = os.path.join(__CONFIG_H5_OPT_DIR__, 'data', 'put.csv')
    df_volume.to_csv(path_csv)


def calc():
    path_csv = os.path.join(__CONFIG_H5_OPT_DIR__, 'data', 'call.csv')
    df_volume_call = pd.read_csv(path_csv, index_col=0, parse_dates=True)
    df_volume_call.columns = ['call']
    path_csv = os.path.join(__CONFIG_H5_OPT_DIR__, 'data', 'put.csv')
    df_volume_put = pd.read_csv(path_csv, index_col=0, parse_dates=True)
    df_volume_put.columns = ['put']
    df_volume = pd.DataFrame()
    df_volume['call'] = df_volume_call['call']
    df_volume['put'] = df_volume_put['put']
    df_volume['cpr'] = df_volume['call'] / df_volume['put']
    df_volume['pcr'] = df_volume['put'] / df_volume['call']
    path_csv = os.path.join(__CONFIG_H5_OPT_DIR__, 'data', 'put_call_ratio.csv')
    df_volume.to_csv(path_csv)

    debug = 1


if __name__ == '__main__':
    main2()
    calc()
