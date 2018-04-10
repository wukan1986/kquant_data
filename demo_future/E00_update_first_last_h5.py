#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
更新数据范围列表，用于下载数据时减少重复下载

日线数据路径是 IF/IF1804.h5
分钟数据路径是 IF1804/IF1804_20180226.h5

已经退市的合约，只要做过一次，后面就没有必要再做了
但数据还是有问题，不活跃的合约，最后几天完全没有行情了
"""
import os
import sys

import pandas as pd
from datetime import datetime
from kquant_data.utils.xdatetime import yyyyMMddHHmm_2_datetime, yyyyMMdd_2_datetime
from kquant_data.config import __CONFIG_H5_FUT_SECTOR_DIR__
from kquant_data.future.symbol import wind_code_2_InstrumentID
from kquant_data.xio.csv import read_datetime_dataframe
from kquant_data.utils.xdatetime import tic, toc

# 解决Python 3.6的pandas不支持中文路径的问题
print(sys.getfilesystemencoding())  # 查看修改前的
try:
    sys._enablelegacywindowsfsencoding()  # 修改
    print(sys.getfilesystemencoding())  # 查看修改后的
except:
    pass

path_ipo_last_trade = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, 'ipo_last_trade_trading.csv')


def get_in_file_day(file_path, wind_code, df):
    print(file_path)
    # 需要
    df_h5 = pd.read_hdf(file_path)
    df_h5.dropna(axis=0, how='all', thresh=3, inplace=True)
    if df_h5.empty:
        return df
    first = yyyyMMddHHmm_2_datetime(df_h5['DateTime'].iloc[0])
    last = yyyyMMddHHmm_2_datetime(df_h5['DateTime'].iloc[-1])

    try:
        df.loc[wind_code]['first'] = min(first, df.loc[wind_code]['first'])
        df.loc[wind_code]['last'] = max(last, df.loc[wind_code]['last'])
    except:
        df.loc[wind_code] = None
        df.loc[wind_code]['first'] = first
        df.loc[wind_code]['last'] = last

    return df


def process_one_file(filename, dirpath, df):
    shotname, extension = os.path.splitext(filename)
    if extension != '.h5':
        return 'continue', df
    try:
        wind_code, date = shotname.split('_')
    except:
        wind_code = shotname
    path_h5 = os.path.join(dirpath, filename)
    # 文件
    fsize = os.path.getsize(path_h5)
    if fsize == 2608:
        return 'continue', df
    print(path_h5)
    # 需要
    df_h5 = pd.read_hdf(path_h5)
    df_h5.dropna(axis=0, how='all', thresh=3, inplace=True)
    if df_h5.empty:
        return 'continue', df
    first = yyyyMMddHHmm_2_datetime(df_h5['DateTime'].iloc[0])
    last = yyyyMMddHHmm_2_datetime(df_h5['DateTime'].iloc[-1])

    try:
        df.loc[wind_code]['first'] = min(first, df.loc[wind_code]['first'])
        df.loc[wind_code]['last'] = max(last, df.loc[wind_code]['last'])
    except:
        df.loc[wind_code] = None
        df.loc[wind_code]['first'] = first
        df.loc[wind_code]['last'] = last
    return 'break', df


def get_in_dir_min(dir_path, df, load_first):
    for _dirpath, _dirnames, _filenames in os.walk(dir_path):
        length = len(_filenames)
        curr_pos = 0
        # 正序得到开始时间
        if load_first:
            for i in range(length):
                curr_pos = i
                filename = _filenames[i]
                control, df = process_one_file(filename, _dirpath, df)
                if control == 'continue':
                    continue
                elif control == 'break':
                    break

        # 逆序得到结束时间
        # for i in range(length - 1, -1, -1):
        for i in reversed(range(curr_pos + 1, length)):
            # print(i)
            filename = _filenames[i]
            control, df = process_one_file(filename, _dirpath, df)
            if control == 'continue':
                continue
            elif control == 'break':
                break

    return df


def get_first_last_min(root_path, date_table, df):
    """
    遍历文件夹，得到合约的数据开始时间与结束时间
    兼容文件夹下都是同一合约，分钟数据
    兼容文件夹下都是同产品，日数据
    :param root_path:
    :return:
    """
    for dirpath, dirnames, filenames in os.walk(root_path):
        for dirname in dirnames:
            # if dirname != 'PM1801':
            #     continue
            dirpath_dirname = os.path.join(dirpath, dirname)
            load_first = False
            try:
                row = date_table.loc[dirname]
                # 最后一天如何判断数据已经完成？
                if row['lasttrade_date'] + pd.Timedelta('14.5h') < row['last']:
                    print(row['last'])
                    continue
            except:
                load_first = True
            df = get_in_dir_min(dirpath_dirname, df, load_first)

    path = os.path.join(root_path, 'first_last.csv')
    df.index.name = 'product'
    df.to_csv(path)
    return df


def get_first_last_day(root_path, date_table, df):
    """
    遍历文件夹，得到合约的数据开始时间与结束时间
    兼容文件夹下都是同一合约，分钟数据
    兼容文件夹下都是同产品，日数据
    :param root_path:
    :return:
    """
    for dirpath, dirnames, filenames in os.walk(root_path):
        for dirname in dirnames:
            dirpath_dirname = os.path.join(dirpath, dirname)
            for _dirpath, _dirnames, _filenames in os.walk(dirpath_dirname):
                for filename in _filenames:
                    dirpath_filename = os.path.join(_dirpath, filename)
                    shotname, extension = os.path.splitext(filename)
                    if extension != '.h5':
                        continue
                    try:
                        row = date_table.loc[shotname]
                        # 最后一天如何判断数据已经完成？
                        if row['lasttrade_date'] == row['last']:
                            print(row['last'])
                            continue
                    except:
                        pass
                    df = get_in_file_day(dirpath_filename, shotname, df)

    path = os.path.join(root_path, 'first_last.csv')
    df.index.name = 'product'
    df.to_csv(path)
    return df


def CZCE_3to4(x):
    if x['exchange'] == 'CZC':
        x['InstrumentID'] = '%s%s%s' % (x['InstrumentID'][0:2], str(x['lasttrade_date'])[2], x['InstrumentID'][-3:])

    return x


def load_ipo_last_trade_trading():
    df_csv = pd.read_csv(path_ipo_last_trade, encoding='utf-8-sig')

    df = wind_code_2_InstrumentID(df_csv, 'wind_code')
    df = df.apply(CZCE_3to4, axis=1)
    df = df.set_index(['InstrumentID'])
    return df


if __name__ == '__main__':
    ipo_last_trade = load_ipo_last_trade_trading()
    ipo_last_trade['ipo_date'] = ipo_last_trade['ipo_date'].apply(lambda x: yyyyMMdd_2_datetime(x))
    ipo_last_trade['lasttrade_date'] = ipo_last_trade['lasttrade_date'].apply(lambda x: yyyyMMdd_2_datetime(x))
    ipo_last_trade['ipo_date'] = pd.to_datetime(ipo_last_trade['ipo_date'])
    ipo_last_trade['lasttrade_date'] = pd.to_datetime(ipo_last_trade['lasttrade_date'])

    root_path = r'D:\DATA_FUT_HDF5\Data_Wind\60'
    csv_path = os.path.join(root_path, 'first_last.csv')
    first_last = read_datetime_dataframe(csv_path)
    if first_last is None:
        first_last = pd.DataFrame(columns=['first', 'last'])

    date_table = ipo_last_trade.merge(first_last, how='inner', left_index=True, right_index=True)

    tic()
    df = get_first_last_min(root_path, date_table, first_last)
    toc()

    # 如何实现
    root_path = r'D:\DATA_FUT_HDF5\Data_Wind\86400_Wind'
    csv_path = os.path.join(root_path, 'first_last.csv')
    first_last = read_datetime_dataframe(csv_path)
    if first_last is None:
        first_last = pd.DataFrame(columns=['first', 'last'])

    date_table = ipo_last_trade.merge(first_last, how='inner', left_index=True, right_index=True)

    tic()
    df = get_first_last_day(root_path, date_table, first_last)
    toc()

