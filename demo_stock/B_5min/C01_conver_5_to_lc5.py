#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
由于直接从通达信客户端中无法下载更早时间的5分钟数据，所以可以从通达信官网上先通过下载软件下载5分钟数据
下载过来的数据格式为5，需可转换成lc5，然后复制到vipdoc/sh/fzline下即可
然后再通过通达信软件的盘后下载功能补全后面的数据即可

以前的方法是将5格式转成h5,将lc5也转成h5，然后将两个h5合并，
现在不搞那么复杂，直接将下载的数据转成lc5，后面只要将通达信合并的lc5，转成h5即可用不着再做别的操作

5分钟数据下载地址
http://www.tdx.com.cn/list_66_69.html
"""
import os
import numpy as np
import pandas as pd
import struct

from ctypes import create_string_buffer

from kquant_data.stock.tdx import read_file


def unpack_records(format, data):
    record_struct = struct.Struct(format)
    return (record_struct.unpack_from(data, offset)
            for offset in range(0, len(data), record_struct.size))


def pack_records(format, data, file):
    record_struct = struct.Struct(format)
    buff = create_string_buffer(len(data) * record_struct.size)
    for idx in range(0, len(data)):
        record_struct.pack_into(buff, idx * record_struct.size, *data[idx])
    file.write(buff)


def int_to_float(x, unit):
    # 将OHLC的价格做调整
    x = list(x)  # tuple转list
    x[1] *= unit
    x[2] *= unit
    x[3] *= unit
    x[4] *= unit
    return x


def min_5_to_lc5(input_file, output_file):
    formats_5 = ['i'] + ['i'] * 4 + ['f'] + ['i'] * 2
    formats_5 = "".join(formats_5)
    formats_lc5 = ['i'] + ['f'] * 4 + ['f'] + ['i'] * 2
    formats_lc5 = "".join(formats_lc5)

    data = []
    # 由于需要将i4转换成f4,得特别处理
    with open(input_file, 'rb') as f:
        content = f.read()
        raw_li = unpack_records(formats_5, content)
        data = list(raw_li)

    #
    columns = ['DateTime', 'Open', 'High', 'Low', 'Close', 'Amount', 'Volume', 'na']
    tmp = pd.DataFrame(data[-10:], columns=columns)
    # 要是没有成交额就惨了
    r = tmp.Amount / tmp.Volume / tmp.Close
    # 为了解决价格扩大了多少倍的问题
    type_unit = np.power(10, np.round(np.log10(r))).median()

    data = list(map(int_to_float, data, [type_unit] * len(data)))

    with open(output_file, 'wb') as f:
        pack_records(formats_lc5, data, f)


if __name__ == '__main__':
    # 先将数据转换格式，然后手工复制即可
    # 以下代码可以复制两次，sh与sz分别处理即可
    input_path = r'D:\test\\5'
    output_path = r'D:\test\\lc5'
    for dirpath, dirnames, filenames in os.walk(input_path, topdown=True):
        for filename in filenames:
            shotname, extension = os.path.splitext(filename)
            if extension != '.5':
                continue
            input_filname = os.path.join(input_path, filename)
            ouput_filname = os.path.join(output_path, '%s.lc5' % shotname)
            min_5_to_lc5(input_filname, ouput_filname)
            print(ouput_filname)
            if True:
                df_5 = read_file(input_filname)
                df_lc5 = read_file(ouput_filname)
                # 下断点用
                debug = 1
