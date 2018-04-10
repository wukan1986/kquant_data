#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
更新数据范围列表，用于下载数据时减少重复下载
"""
import os
import sys
from datetime import datetime

import pandas as pd

# 解决Python 3.6的pandas不支持中文路径的问题
print(sys.getfilesystemencoding())  # 查看修改前的
try:
    sys._enablelegacywindowsfsencoding()  # 修改
    print(sys.getfilesystemencoding())  # 查看修改后的
except:
    pass


def get_first_last(root_path):
    """
    遍历文件夹，得到合约的数据开始时间与结束时间
    兼容文件夹下都是同一合约，分钟数据
    兼容文件夹下都是同产品，日数据
    :param root_path:
    :return:
    """
    df = pd.DataFrame(columns=['first', 'last'])

    for dirpath, dirnames, filenames in os.walk(root_path):
        for dirname in dirnames:
            dirpath_dirname = os.path.join(dirpath, dirname)
            for _dirpath, _dirnames, _filenames in os.walk(dirpath_dirname):
                length = len(_filenames)
                for index, filename in enumerate(_filenames):
                    shotname, extension = os.path.splitext(filename)
                    try:
                        wind_code, date = shotname.split('_')
                        if 0 < index < length - 1:
                            continue
                    except:
                        wind_code = shotname

                    path_csv = os.path.join(_dirpath, filename)
                    df_csv = pd.read_csv(path_csv, index_col=0, parse_dates=True)
                    # 删除无效行
                    # 南华在收盘前只能下到收盘价，并且收盘价还是昨天的
                    df_csv.dropna(axis=0, how='all', thresh=3, inplace=True)
                    if df_csv.empty:
                        continue

                    first = df_csv.index[0]
                    last = df_csv.index[-1]

                    try:
                        df.loc[wind_code]['first'] = min(first, df.loc[wind_code]['first'])
                        df.loc[wind_code]['last'] = max(last, df.loc[wind_code]['last'])
                    except:
                        df.loc[wind_code] = None
                        df.loc[wind_code]['first'] = first
                        df.loc[wind_code]['last'] = last

    # 直接保存在当前目录下
    path = os.path.join(root_path, 'first_last.csv')
    df.index.name = 'wind_code'
    df.to_csv(path)
    return df


if __name__ == '__main__':
    root_path = r'D:\DATA_FUT\86400'
    df = get_first_last(root_path)

    root_path = r'D:\DATA_FUT\60'
    df = get_first_last(root_path)
