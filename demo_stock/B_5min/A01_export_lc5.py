#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
lc5数据转换成h5数据放在指定目录
目前指定导出到5min_lc5
由于还得通过与5min的合并，所以这里只导出数据，不做复权处理
"""
import os
import pandas as pd

from kquant_data.config import __CONFIG_H5_STK_DIR__, __CONFIG_TDX_STK_DIR__
from kquant_data.stock.symbol import get_symbols_from_path_only_stock
from kquant_data.stock.tdx import get_tdx_path, bars_to_h5
from kquant_data.stock.stock import read_file
from kquant_data.processing.utils import multiprocessing_convert


def _export_data(rule, _input, output, instruments, i):
    t = instruments.iloc[i]
    print("%d %s" % (i, t['local_symbol']))

    _tdx_path = os.path.join(__CONFIG_TDX_STK_DIR__, get_tdx_path(t['market'], t['code'], 300))
    try:
        df = read_file(_tdx_path)
    except FileNotFoundError:
        # 没有原始的数据文件
        return None

    # 导出到临时目录时因子都用1
    df['backward_factor'] = 1
    df['forward_factor'] = 1
    data_output = os.path.join(__CONFIG_H5_STK_DIR__, output, t['market'], "%s.h5" % t['local_symbol'])
    bars_to_h5(data_output, df)


def export_symbols():
    path = os.path.join(__CONFIG_TDX_STK_DIR__, "vipdoc", 'sh', 'fzline')
    df_sh = get_symbols_from_path_only_stock(path, "SSE")
    path = os.path.join(__CONFIG_TDX_STK_DIR__, "vipdoc", 'sz', 'fzline')
    df_sz = get_symbols_from_path_only_stock(path, "SZSE")
    df = pd.concat([df_sh, df_sz])

    return df


if __name__ == '__main__':
    _input = 'fzline'
    _ouput = '5min_lc5'
    # 由于直接5转lc5格式已经提前做了，所以这里不再需要都保存成h5再合并了
    _ouput = '5min'
    instruments = export_symbols()

    multiprocessing_convert(True, '5min', _input, _ouput, instruments, _export_data)

    # 下断点用
    debug = 1
