#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
处理通达信数据相关的操作

http://www.cnblogs.com/zeroone/archive/2013/07/10/3181251.html
文件路径
-------
D:\new_hbzq\vipdoc\sh\lday
D:\new_hbzq\vipdoc\sh\fzline
D:\new_hbzq\vipdoc\sh\minline

"""
import os

import numpy as np
import pandas as pd

from ..xio.h5 import write_dataframe_set_struct_keep_head
from ..utils.xdatetime import yyyyMMddHHmm_2_datetime

# 保存成h5格式时的类型
tdx_h5_type = np.dtype([
    ('DateTime', np.uint64),
    ('Open', np.float32),
    ('High', np.float32),
    ('Low', np.float32),
    ('Close', np.float32),
    ('Amount', np.float32),
    ('Volume', np.uint32),
    ('backward_factor', np.float32),
    ('forward_factor', np.float32),
])


def bars_to_h5(input_path, data):  # 保存日线
    write_dataframe_set_struct_keep_head(input_path, data, tdx_h5_type, 'BarData')
    return


def min_datetime_long(dt):
    """
    传入分钟，输出为yyyyMMddHHmm
    :param dt:
    :return:
    """
    # 由于dt << 16会由int变成long，所以转换失败
    (tnum, dnum) = divmod(dt, 1 << 16)  # 2**16
    (ym, res) = divmod(dnum, 2048)
    y = ym + 2004
    # (m, d) = divmod(res, 100)
    h, t = divmod(tnum, 60)

    return float(y * 100000000.0 + res * 10000.0 + h * 100 + t * 1)


def day_datetime_long(dt):
    """
    传入的数据是日，需要转成分钟,输出为yyyyMMddHHmm
    :param dt:
    :return:
    """
    return float(dt * 10000.0)


def read_file(path):
    """
    http://www.tdx.com.cn/list_66_68.html
    通达信本地目录有day/lc1/lc5三种后缀名，两种格式
    从通达信官网下载的5分钟后缀只有5这种格式，为了处理方便，时间精度都只到分钟
    :param path:
    :return:
    """
    file_ext = os.path.splitext(path)[1][1:]
    ohlc_type = {'day': 'i4', '5': 'i4', 'lc1': 'f4', 'lc5': 'f4'}[file_ext]
    date_parser = {'day': day_datetime_long,
                   '5': min_datetime_long,
                   'lc1': min_datetime_long,
                   'lc5': min_datetime_long,
                   }[file_ext]
    columns = ['DateTime', 'Open', 'High', 'Low', 'Close', 'Amount', 'Volume', 'na']
    formats = ['i4'] + [ohlc_type] * 4 + ['f4'] + ['i4'] * 2
    dtype = np.dtype({'names': columns, 'formats': formats})
    data = np.fromfile(path, dtype=dtype)
    df = pd.DataFrame(data)
    # 为了处理的方便，存一套long类型的时间
    df['DateTime'] = df['DateTime'].apply(date_parser)
    df['datetime'] = df['DateTime'].apply(yyyyMMddHHmm_2_datetime)
    df = df.set_index('datetime')
    df = df.drop('na', 1)

    # 有两种格式的数据的价格需要调整
    if file_ext == 'day' or file_ext == '5':
        tmp = df.tail(10)
        r = tmp.Amount / tmp.Volume / tmp.Close
        # 为了解决价格扩大了多少倍的问题
        type_unit = np.power(10, np.round(np.log10(r))).median()
        # 这个地方要考虑到实际情况，不要漏价格，也不要把时间做了除法
        df.ix[:, 1:5] = df.ix[:, 1:5] * type_unit

    # 转换格式，占用内存更少
    df['DateTime'] = df['DateTime'].astype(np.uint64)
    df['Open'] = df['Open'].astype(np.float32)
    df['High'] = df['High'].astype(np.float32)
    df['Low'] = df['Low'].astype(np.float32)
    df['Close'] = df['Close'].astype(np.float32)
    df['Amount'] = df['Amount'].astype(np.float32)
    df['Volume'] = df['Volume'].astype(np.uint32)

    # print(df.dtypes)

    return df


def file_ext_2_bar_size(file_ext):
    """

    :param file_ext:
    :return:
    """
    ret = {
        'day': 86400,
        '5': 300,
        'lc1': 60,
        'lc5': 300,
    }[file_ext]
    return ret


def bar_size_2_file_ext(bar_size):
    ret = {
        86400: 'day',
        300: 'lc5',
        60: 'lc5',
        5: '5',  # 这是为了读特殊的从通达信网站上下载的数据
    }[bar_size]
    return ret


def bar_size_2_folder(bar_size):
    ret = {
        86400: 'lday',
        300: 'fzline',
        60: 'minline',
        5: '5',  # 这是为了读特殊的从通达信网站上下载的数据
    }[bar_size]
    return ret


def get_tdx_path(market, code, bar_size):
    # D:\new_hbzq\vipdoc\sh\lday\sh000001.day
    # D:\new_hbzq\vipdoc\sh\fzline\sh000001.lc5
    # D:\new_hbzq\vipdoc\sh\minline\sh000001.lc1
    folder = bar_size_2_folder(bar_size)
    file_ext = bar_size_2_file_ext(bar_size)
    filename = "%s%s.%s" % (market, code, file_ext)
    return os.path.join("vipdoc", market, folder, filename)


def get_h5_86400_path(market, code, bar_size):
    # D:\DATA_STK_HDF5\daily\sh\sh000001.h5
    filename = "%s%s.h5" % (market, code)
    return os.path.join("1day", market, filename)
