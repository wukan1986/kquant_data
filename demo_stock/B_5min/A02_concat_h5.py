#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
将新生成的5分钟数据与老的5分钟数据进行合并
合并出来的数据只用于生成5分钟的单文件数据时使用，其它情况下不使用
"""
import os
import pandas as pd

from kquant_data.config import __CONFIG_H5_STK_DIR__, __CONFIG_H5_STK_DIVIDEND_DIR__
from kquant_data.stock.stock import merge_adjust_factor, bars_to_h5
from kquant_data.processing.utils import filter_dataframe, multiprocessing_convert
from kquant_data.stock.symbol import get_folder_symbols


def _export_data(rule, _input, output, instruments, i):
    t = instruments.iloc[i]
    print("%d %s" % (i, t['local_symbol']))

    path_new = os.path.join(__CONFIG_H5_STK_DIR__, _input, t['market'], "%s.h5" % t['local_symbol'])
    # 这里不应当出错，因为之前已经导出过数据到
    df_new = pd.read_hdf(path_new)
    if df_new is None:
        return None
    df_new = filter_dataframe(df_new, 'DateTime', None, None, None)

    path_old = os.path.join(__CONFIG_H5_STK_DIR__, output, t['market'], "%s.h5" % t['local_symbol'])
    try:
        # 没有以前的数据
        df_old = pd.read_hdf(path_old)
        if df_old is None:
            df = df_new
        else:
            df_old = filter_dataframe(df_old, 'DateTime', None, None, None)
            # 数据合并，不能简单的合并
            # 需要保留老的，新的重复的地方忽略
            last_ts = df_old.index[-1]
            df_new2 = df_new[last_ts:][1:]

            df = pd.concat([df_old, df_new2])
    except:
        df = df_new

    # 有可能没有除权文件
    div_path = os.path.join(__CONFIG_H5_STK_DIVIDEND_DIR__, "%s.h5" % t['local_symbol'])
    try:
        div = pd.read_hdf(div_path)
        div = filter_dataframe(div, 'time', None, None, None)
        df = merge_adjust_factor(df, div)
    except:
        # 这里一般是文件没找到，表示没有除权信息
        df['backward_factor'] = 1
        df['forward_factor'] = 1

    bars_to_h5(path_old, df)


if __name__ == '__main__':
    # 此合并h5的代码已经废弃不用
    _input = '5min_lc5'
    _ouput = '5min'
    instruments = get_folder_symbols(__CONFIG_H5_STK_DIR__, _input)

    multiprocessing_convert(True, '5min', _input, _ouput, instruments, _export_data)

    # 下断点用
    debug = 1
