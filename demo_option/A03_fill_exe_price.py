#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
填充执行价

先将历史上的执行价填上，再将最新的执行价填上
再从下向上填充即可
"""
import os
import numpy as np
import pandas as pd

from kquant_data.api import get_price
from kquant_data.option.info import get_opt_info, get_opt_info_filtered_by_month
from kquant_data.processing.utils import read_fill_from_dir

if __name__ == '__main__':
    # 获取期权基础信息文件
    df_info = get_opt_info('510050.SH.csv')

    df_filtered = get_opt_info_filtered_by_month(df_info, 1806)

    df_filtered = df_filtered[(df_filtered['listed_date'] <= '2018-04-10')]

    pricing = get_price(list(df_filtered.index), path=r'D:\shqqday', instrument_type='option')
    # print(df)

    # 这是期权的价格，不是标的的价格
    Close = pricing['Close']
    # print(Close)

    df_strike_price = pd.DataFrame(index=Close.index, columns=Close.columns)
    df_strike_price = read_fill_from_dir(r'D:\DATA_OPT\optionchain\510050.SH\\', 'strike_price', df_strike_price)
    df_strike_price.iloc[-1] = df_info['exercise_price']
    df_strike_price.fillna(method='bfill', inplace=True)

    df_multiplier = pd.DataFrame(index=Close.index, columns=Close.columns)
    df_multiplier = read_fill_from_dir(r'D:\DATA_OPT\optionchain\510050.SH\\', 'multiplier', df_multiplier)
    df_multiplier.iloc[-1] = df_info['contract_unit']
    df_multiplier.fillna(method='bfill', inplace=True)

    df_info['call_or_put'] = df_info['call_or_put'].replace('C', 1).replace('P', -1)
    df_cp = pd.DataFrame(index=Close.index, columns=Close.columns)
    df_cp.iloc[-1] = df_info['call_or_put']
    df_cp.fillna(method='bfill', inplace=True)

    df_50etf = pd.DataFrame(index=Close.index, columns=Close.columns)
    # 取标的价格
    df2 = get_price('510050.SH', instrument_type='stock', fields=['Close'])

    df_50etf.iloc[:, 0] = df2
    df_50etf = df_50etf.T.fillna(method='ffill').T

    # 标记实虚值
    atm = pd.DataFrame(0, index=Close.index, columns=Close.columns)
    atm[df_50etf < df_strike_price] = 1
    atm[df_50etf > df_strike_price] = -1
    atm[np.isnan(Close)] = np.nan
    atm = atm * df_cp

    # 上面是按

    debug = 1
