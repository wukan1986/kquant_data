#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
由于直接从通达信客户端中无法下载更早时间的5分钟数据，所以可以从通达信官网上先通过下载软件下载5分钟数据
然后导出，最后再合并即可

5分钟数据下载地址
http://www.tdx.com.cn/list_66_69.html

建立与fzline同级目录的5文件夹，将数据复制进去
运行当前程序，并转换到5min_5
现在5min_lc5是最新的数据，只要将5min_5中的数据复制到5min中，然后执行合并的脚本即可
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

    _tdx_path = os.path.join(__CONFIG_TDX_STK_DIR__, get_tdx_path(t['market'], t['code'], 5))
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
    path = os.path.join(__CONFIG_TDX_STK_DIR__, "vipdoc", 'sh', '5')
    df_sh = get_symbols_from_path_only_stock(path, "SSE")
    path = os.path.join(__CONFIG_TDX_STK_DIR__, "vipdoc", 'sz', '5')
    df_sz = get_symbols_from_path_only_stock(path, "SZSE")
    df = pd.concat([df_sh, df_sz])

    return df


if __name__ == '__main__':
    _input = '5'
    _ouput = '5min_5'
    instruments = export_symbols()

    multiprocessing_convert(True, '5min', _input, _ouput, instruments, _export_data)

    # 下断点用
    debug = 1
