#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
演示将5分钟数据转成1小时数据
"""
import os
import pandas as pd

from kquant_data.config import __CONFIG_H5_STK_DIR__
from kquant_data.stock.stock import bars_to_h5
from kquant_data.processing.utils import filter_dataframe, bar_convert, multiprocessing_convert
from kquant_data.stock.symbol import get_folder_symbols


def _export_data(rule, _input, output, instruments, i):
    t = instruments.iloc[i]
    print("%d %s" % (i, t['local_symbol']))
    path = os.path.join(__CONFIG_H5_STK_DIR__, _input, t['market'], "%s.h5" % t['local_symbol'])
    df = None
    try:
        df = pd.read_hdf(path)
    except:
        pass
    if df is None:
        return None
    df = filter_dataframe(df, 'DateTime', None, None, None)
    df1 = bar_convert(df, rule)
    date_output = os.path.join(__CONFIG_H5_STK_DIR__, output, t['market'], "%s.h5" % t['local_symbol'])
    bars_to_h5(date_output, df1)


if __name__ == '__main__':
    _input = '5min'
    _ouput = '1hour'
    instruments = get_folder_symbols(__CONFIG_H5_STK_DIR__, _input)

    multiprocessing_convert(True, '1H', _input, _ouput, instruments, _export_data)

    # 下断点用
    debug = 1
