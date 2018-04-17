#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据处理的小工具
"""
import os
import datetime
import numpy as np
import pandas as pd
import multiprocessing
from functools import partial

from ..utils.xdatetime import yyyyMMddHHmm_2_datetime, datetime_2_yyyyMMddHHmm, tic, toc
from ..xio.csv import read_data_dataframe


def expand_dataframe_according_to(df, index, columns):
    """
    将dataframe按指定的index和columns扩展
    它的优点是，非交易日的数据也能扩展到交易日
    它用ffill填充了后面的数据，如果那天其实真的没有数据会导致问题
    :param df:
    :param index:
    :param columns:
    :return:
    """
    # 扩展数据，原有数据是一个稀疏矩阵，现在通过新的index与columns扩展成更合适的数据大小
    df_ = pd.DataFrame(index, index=index, columns=['_index_'])
    df_ = pd.merge(df_, df, how='outer', left_index=True, right_index=True)
    del df_['_index_']
    # fillna的方法必需在slice方法前面执行，因为有可能slice时会删除了关键点,导致以后loc时缺数据
    df_.fillna(method='ffill', inplace=True)
    df_ = df_.loc[index]
    # 可能columns中有新的列，但数据中没有,所以要建一个新数据
    df = pd.DataFrame(df_, index=df_.index, columns=columns)
    df[df_.columns] = df_
    df = df[columns]
    return df


def rolling_count(val):
    """
    相当于cumcount功能
    :param val:
    :return:
    """
    if val == rolling_count.previous:
        rolling_count.count += 1
    else:
        rolling_count.previous = val
        rolling_count.count = 0
    return rolling_count.count


rolling_count.count = 0  # static variable
rolling_count.previous = None  # static variable


def series_drop_duplicated_keep_both_rolling(series):
    """
    删除重复，只保留前后两端的数据
    如果中间出现重复数据也能使用了
    :param series:
    :return:
    """
    rolling_count.previous = None
    _count_ = series.apply(rolling_count)
    _first_ = _count_ == 0
    _last_ = _first_.shift(-1)
    _last_[-1] = True

    series = series[_first_ | _last_]
    return series


def filter_dataframe(df, index_name=None, start_date=None, end_date=None, fields=None):
    if index_name is not None:
        df['index_datetime'] = df[index_name].apply(yyyyMMddHHmm_2_datetime)
        df = df.set_index('index_datetime')
    # 过滤时间
    if start_date is not None or end_date is not None:
        df = df[start_date:end_date]
    # 过滤字段
    if fields is not None:
        df = df[fields]
    return df


def split_into_group(arr, n):
    out = [arr[i:i + n] for i in range(0, len(arr), n)]
    return out


def ndarray_to_dataframe(arr, index, columns, start=None, end=None):
    df = pd.DataFrame(arr, index=index, columns=columns)
    df = df[start:end]
    return df


def bar_convert(df, rule='1H'):
    """
    数据转换
    :param df:
    :param rule:
    :return:
    """
    how_dict = {
        'DateTime': 'first',
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Amount': 'sum',
        'Volume': 'sum',
        'backward_factor': 'last',
        'forward_factor': 'last',
    }
    # how_dict = {
    #     'Symbol': 'first',
    #     'DateTime': 'first',
    #     'TradingDay': 'first',
    #     'ActionDay': 'first',
    #     'Time': 'first',
    #     'BarSize': 'first',
    #     'Pad': 'min',
    #     'Open': 'first',
    #     'High': 'max',
    #     'Low': 'min',
    #     'Close': 'last',
    #     'Volume': 'sum',
    #     'Amount': 'sum',
    #     'OpenInterest': 'last',
    #     'Settle': 'last',
    #     'AdjustFactorPM': 'last',
    #     'AdjustFactorTD': 'last',
    #     'BAdjustFactorPM': 'last',
    #     'BAdjustFactorTD': 'last',
    #     'FAdjustFactorPM': 'last',
    #     'FAdjustFactorTD': 'last',
    #     'MoneyFlow': 'sum',
    # }
    columns = df.columns
    new = df.resample(rule, closed='left', label='left').apply(how_dict)

    new.dropna(inplace=True)
    # 有些才上市没多久的，居然开头有很多天为空白，需要删除,如sh603990
    new = new[new['Open'] != 0]

    # 居然位置要调整一下
    new = new[columns]

    # 由于存盘时是用的DateTime这个字段，不是index上的时间，这会导致其它软件读取数据时出错，需要修正数据
    new['DateTime'] = new.index.map(datetime_2_yyyyMMddHHmm)

    return new


def multiprocessing_convert(multi, rule, _input, output, instruments, func_convert):
    tic()

    if multi:
        pool_size = multiprocessing.cpu_count() - 1
        pool = multiprocessing.Pool(processes=pool_size)
        func = partial(func_convert, rule, _input, output, instruments)
        pool_outputs = pool.map(func, range(len(instruments)))
        print('Pool:', pool_outputs)
    else:
        for i in range(len(instruments)):
            func_convert(rule, _input, output, instruments, i)

    toc()



def dataframe_align_copy(df1, df2):
    """
    两个DataFrame，将其中的数据复制到另一个
    :param df1:
    :param df2:
    :return:
    """
    index = df1.index.intersection(df2.index)
    columns = df1.columns.intersection(df2.columns)

    # 由于两边的数据不配套，所以只能复制重合部分
    df1.ix[index, columns] = df2.ix[index, columns]

    return df1


def read_fill_from_file(path, date, field, df):
    """
    将一个文件中的内容合并到一个df中
    :param path:
    :param date:
    :param field:
    :param df:
    :return:
    """
    _path = path % date
    x = read_data_dataframe(_path)
    # 去掉.SH
    x['wind_code'] = x['option_code'].str.extract('(\d{8})\.', expand=False)
    x.set_index(['wind_code'], inplace=True)
    y = x[[field]]
    y.columns = [date]
    y = y.T
    y.index = pd.to_datetime(y.index)

    df = dataframe_align_copy(df, y)
    return df


def read_fill_from_dir(path, field, df):
    """
    将一个目录下的合并到一个df中
    :param path:
    :param field:
    :param df:
    :return:
    """
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            shotname, extension = os.path.splitext(filename)
            _path = os.path.join(dirpath, '%s.csv')
            df = read_fill_from_file(_path, shotname, field, df)
    return df