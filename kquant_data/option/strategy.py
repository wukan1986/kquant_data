#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

"""
import numpy as np

from .info import get_opt_info_filtered_by_date, get_opt_info_filtered_by_month, \
    get_opt_info_filtered_by_call_or_put


def func_select_instrument_by_signal(row, field, _open, df_info, month_index, atm_index):
    """
    # 用开盘价来选要交易的合约代码
    long_short_opt = long_short_enter.apply(func_select_instrument_by_signal, axis=1,
                                            args=('SZ50', Open, df_info, 3, 1))
    long_short_opt.set_index(['index_datetime'], inplace=True)

    由于比较特别，还是需要用户自己选才行
    :param row: 每行信号
    :param field: 信号在当前数据中的字段名
    :param _open: 交易当天的开盘价
    :param df_info: 期权合约信息表，将通过etf行情和执行价选出实虚合约
    :param month_index: 选出当月、次月、当季、次季
    :param atm_index: 选出虚几
    :return:
    """
    signal = row[field]
    if np.isnan(signal):
        return row
    if signal == 0:
        row[field] = np.nan
        return row
    # 取指定日期的数据
    date = row['index_datetime'].strftime("%Y-%m-%d")  # 指定日期

    # 2015-05-29号，5月份的合约已经结束，应当切换成6月才行
    df_filtered = get_opt_info_filtered_by_date(df_info, date)
    df_filtered1 = df_filtered.reset_index()
    # 除权时，选合约会有问题，如果改成除权后，有新开仓才去新合约即可
    df_filtered1.set_index(['limit_month', 'limit_month_m'], inplace=True)
    list_limit_month = list(set(df_filtered1.index.get_level_values(0)))
    list_limit_month.sort()

    # 设定是做哪个类型
    # 看多，买入call
    # 看空，买入put
    call_or_put = 'C' if signal > 0 else 'P'
    df_filtered2 = get_opt_info_filtered_by_call_or_put(df_filtered, call_or_put)

    # 选择当月，次月，当季，下季
    # 有可能选出来的合约没有虚值，或没有实值，因为当天新挂合约并知道
    df_filtered3 = get_opt_info_filtered_by_month(df_filtered2, list_limit_month[month_index])

    # 数据正好只有一列，当天开盘时，基本可以确定是否挂了新合约
    px = _open.ix[date, 0]

    # 选出虚值期权
    if call_or_put == 'C':
        # 执行价大的是虚值期权
        infos = df_filtered3[df_filtered3['exercise_price'] > px]
        infos.sort_values(by=['exercise_price'], ascending=[True], inplace=True)
    else:
        # 执行价小的是虚值期权
        infos = df_filtered3[df_filtered3['exercise_price'] < px]
        # 排序一下，按离atm的距离排序
        infos.sort_values(by=['exercise_price'], ascending=[False], inplace=True)

    print(date)
    print(px)
    print(infos.shape[0])

    infos = infos.reset_index()
    # 选出第二
    n = atm_index
    if infos.shape[0] <= 2:
        n = 0
    wind_code = infos.ix[n, 'wind_code']
    row[field] = wind_code

    return row


def func_concat_by_column(row, field, df, df_sel):
    """
    将多个数据合并成一个数据，由于合并的问题，只能先都转成收益率

    df_sel_ret = df_sel.pct_change()

    df_ret = pd.DataFrame(index=long_short_opt.index, columns=long_short_opt.columns)
    long_short_opt_dropna.reset_index(inplace=True)
    long_short_opt_dropna.apply(func_concat_by_column, axis=1, args=('SZ50', df_ret, df_sel_ret))

    :param row: 每行数据，主要是被挑选的合约名
    :param field: 信号在当前数据中的字段名
    :param df: 要被填充的单列数据
    :param df_sel: 存着多列数据
    :return:
    """
    wind_code = row[field]
    date = row['index_datetime']

    df[date:] = df_sel.ix[date:, [wind_code]]

    return row
