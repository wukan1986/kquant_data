#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
进行数据加载与处理
单个文件的加载并没有什么难度，需要处理多个文件的加载
"""

import collections
import os

import pandas as pd
from .utils.symbol import split_by_dot
from .config import __CONFIG_H5_STK_DIR__, __CONFIG_TDX_STK_DIR__, __CONFIG_H5_STK_DIVIDEND_DIR__
from .future.future import read_h5
from .processing.utils import filter_dataframe
from .stock.stock import read_h5_tdx


def _get_date_from_file(symbol, market, code, bar_size, start_date, end_date, fields):
    market = market.upper()

    if market == 'SH' or market == 'SZ':
        # 从通达信中获取
        df = read_h5_tdx(market, code, bar_size, __CONFIG_H5_STK_DIR__, __CONFIG_TDX_STK_DIR__,
                         __CONFIG_H5_STK_DIVIDEND_DIR__)
    else:
        df = read_h5(market, code, bar_size)

    # 除完权后再过滤，主要为修正时间
    df = filter_dataframe(df, 'DateTime', start_date, end_date, fields)

    return df


def get_price(symbols, start_date=None, end_date=None, bar_size=86400, fields=None):
    """
    从通达信中取数据，没有除权的可以直接读，但除权的需要拿到除权因子才能用
    这样的话相当于日线需要转一次，所以可以在本地为日线存一次HDF5
    通达信中目前只有日线、5分钟、1分钟
    :param symbols:
    :param start_date:
    :param end_date:
    :param bar_size:
    :param fields:
    :param adjust_type:
    :return:
    """
    # 将str转成list
    if isinstance(symbols, str):
        symbols = [symbols]
    if isinstance(fields, str):
        fields = [fields]

    _dict = collections.OrderedDict()
    _fields = None
    for symbol in symbols:
        code, market = split_by_dot(symbol)

        df = _get_date_from_file(symbol, market, code, bar_size, start_date, end_date, fields)
        _fields = df.columns
        _dict[symbol] = df

    # 只有一个合约，直接输出单个合约的表
    if len(_dict) == 1:
        return list(_dict.values())[0]

    # 只有一个字段时，输出每个合约的列表
    if len(_fields) == 1:
        dfs = None
        for (k, v) in _dict.items():
            if dfs is None:
                dfs = v
            else:
                dfs = pd.merge(dfs, v, left_index=True, right_index=True, how='outer')
        dfs.columns = list(_dict.keys())
        # 需要将不同合约的进行合并
        return dfs

    # 三维的，需要转换成Panel
    # 是否可以使用 pan = pan_.transpose(2,1,0)，发现不能用，因为没有对齐
    _dict2 = collections.OrderedDict()
    for (k, v) in _dict.items():
        for c in v.columns:
            _df = pd.DataFrame(v[c])
            if c in _dict2:
                _dict2[c] = pd.merge(_dict2[c], _df, left_index=True, right_index=True, how='outer')
            else:
                _dict2[c] = _df

    for (k, v) in _dict2.items():
        v.columns = symbols
        # v.columns = list(_dict.keys())

    return pd.Panel(_dict2)


def all_instruments(path=None, type=None):
    """
    得到合约列表
    :param type:
    :return:
    """
    if path is None:
        path = os.path.join(__CONFIG_H5_STK_DIR__, "daily", 'Symbol.csv')

    df = pd.read_csv(path, dtype={'code': str})

    return df


def get_datetime(path):
    dt = pd.read_csv(path, index_col=0, parse_dates=True)
    dt['date'] = dt.index
    return dt
