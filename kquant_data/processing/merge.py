#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
指定数据目录，生成对应的合约行业数据
"""
import os

import numpy as np
import pandas as pd

from ..api import all_instruments, get_datetime
from ..config import __CONFIG_H5_STK_DIR__, __CONFIG_H5_STK_SECTOR_DIR__, __CONFIG_H5_STK_FACTOR_DIR__
from .utils import expand_dataframe_according_to
from ..utils.xdatetime import tic, toc
from ..wind.wset import read_sectorconstituent_from_dir
from ..xio.h5 import write_dataframe_set_dtype_remove_head
from ..xio.csv import read_data_dataframe


def load_sector(path, value):
    """
    加载指定目录的数据，并标记
    通过unstack将数字设置成值
    :param path:
    :param value:
    :return:
    """
    df = read_sectorconstituent_from_dir(path)
    df['value'] = value
    df = df.set_index(df['_datetime_'])
    df = df.set_index([df.index, df['wind_code']], drop=False)
    df = df['value'].unstack()

    return df


def load_sectors(fold_path):
    """
    带子目录数据的加载
    将数据ID设置成值
    :param fold_path:
    :return:
    """
    path_ = '%s.csv' % fold_path
    sectors = pd.read_csv(path_, encoding='utf-8-sig')

    df = None
    for i in range(0, len(sectors)):
        print(sectors.iloc[i, :])

        sec_name = sectors.ix[i, 'sec_name']
        id = sectors.ix[i, 'ID']

        foldpath = os.path.join(fold_path, sec_name)
        df_tmp = read_sectorconstituent_from_dir(foldpath)
        df_tmp['value'] = id
        if df is None:
            df = df_tmp
        else:
            df = pd.concat([df, df_tmp])

    df = df.set_index(df['_datetime_'])
    df = df.set_index([df.index, df['wind_code']], drop=False)
    df = df['value'].unstack()

    return df


def merge_sector(rule, sector_name, dataset_name):
    """
    合并一级文件夹
    :param rule:
    :param sector_name:
    :param dataset_name:
    :return:
    """
    path = os.path.join(__CONFIG_H5_STK_DIR__, rule, 'Symbol.csv')
    symbols = all_instruments(path)

    path = os.path.join(__CONFIG_H5_STK_DIR__, rule, 'DateTime.csv')
    DateTime = get_datetime(path)

    tic()
    path = os.path.join(__CONFIG_H5_STK_SECTOR_DIR__, sector_name)
    df = load_sector(path, 1)
    print("数据加载完成")
    toc()

    df = expand_dataframe_according_to(df, DateTime.index, symbols['wind_code'])
    # 有些股票从来没有被ST过，比如浦发银行，或一些新股
    df.fillna(0, inplace=True)

    print("数据加载完成")
    toc()

    path = os.path.join(__CONFIG_H5_STK_DIR__, rule, "%s.h5" % dataset_name)
    write_dataframe_set_dtype_remove_head(path, df, np.int8, dataset_name)

    toc()


def merge_sectors(rule, sector_name, dataset_name):
    """
    合并二级文件夹
    :param rule:
    :param sector_name:
    :param dataset_name:
    :return:
    """
    path = os.path.join(__CONFIG_H5_STK_DIR__, rule, 'Symbol.csv')
    symbols = all_instruments(path)

    path = os.path.join(__CONFIG_H5_STK_DIR__, rule, 'DateTime.csv')
    DateTime = get_datetime(path)

    tic()
    path = os.path.join(__CONFIG_H5_STK_SECTOR_DIR__, sector_name)
    df = load_sectors(path)
    print("数据加载完成")
    toc()

    df = expand_dataframe_according_to(df, DateTime.index, symbols['wind_code'])
    # df.to_csv(r"D:\1.csv")
    # 有些股票从来没有被ST过，比如浦发银行，或一些新股
    df.fillna(0, inplace=True)

    print("数据加载完成")
    toc()

    path = os.path.join(__CONFIG_H5_STK_DIR__, rule, "%s.h5" % dataset_name)
    write_dataframe_set_dtype_remove_head(path, df, np.int16, dataset_name)

    toc()


def merge_report(rule, field, dataset_name):
    path = os.path.join(__CONFIG_H5_STK_DIR__, rule, 'Symbol.csv')
    symbols = all_instruments(path)

    path = os.path.join(__CONFIG_H5_STK_DIR__, rule, 'DateTime.csv')
    DateTime = get_datetime(path)

    tic()
    path = os.path.join(__CONFIG_H5_STK_FACTOR_DIR__, field)
    df = None
    cnt = 0
    for dirpath, dirnames, filenames in os.walk(path):
        # 只处理目录
        for filename in filenames:
            print(filename)
            filepath = os.path.join(dirpath, filename)
            df_ = read_data_dataframe(filepath)
            df_tmp = df_.stack()
            if df is None:
                df = df_tmp
            else:
                df = pd.concat([df, df_tmp])
            cnt += 1
            # if cnt > 10:
            #    break

    df = df.unstack()
    df.fillna(method='ffill', inplace=True)
    print("数据加载完成")
    toc()

    df = expand_dataframe_according_to(df, DateTime.index, symbols['wind_code'])
    #
    path = os.path.join(__CONFIG_H5_STK_DIR__, rule, "%s.h5" % dataset_name)
    write_dataframe_set_dtype_remove_head(path, df, None, field)
