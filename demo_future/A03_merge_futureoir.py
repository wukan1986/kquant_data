#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
将持仓数据合并
"""
import os
import pandas as pd
from kquant_data.config import __CONFIG_H5_FUT_DATA_DIR__


def process1(input_path, output_path, member_name, folder_name):
    # 由于文件数太多，从头加载很慢，需要增量处理
    for dirpath, dirnames, filenames in os.walk(input_path):
        for dirname in dirnames:
            # 由于read_csv时不支持中文，只好改用英文
            path2 = os.path.join(output_path, folder_name, '%s.csv' % dirname)
            try:
                df_old = pd.read_csv(path2, encoding='utf-8-sig', parse_dates=True, index_col=['date'])
                last_date_csv = df_old.index[-1].strftime('%Y-%m-%d.csv')
                dfs = df_old
            except:
                last_date_csv = '1900-01-01.csv'
                dfs = None

            sub_dirpath = os.path.join(dirpath, dirname)
            print('开始处理', sub_dirpath)
            for _dirpath, _dirnames, _filenames in os.walk(sub_dirpath):
                for _filename in _filenames:
                    if _filename <= last_date_csv:
                        continue
                    path = os.path.join(_dirpath, _filename)
                    df = pd.read_csv(path, encoding='utf-8-sig', parse_dates=True, index_col=['date'])
                    row = df[df['member_name'] == member_name]
                    dfs = pd.concat([dfs, row])

            # dfs.set_index('date')
            dfs.to_csv(path2, encoding='utf-8-sig', date_format='%Y-%m-%d')
            print("处理完成", path2)


def process2(input_path, output_path):
    for dirpath, dirnames, filenames in os.walk(input_path):
        dfs_long = None
        dfs_short = None
        for filename in filenames:
            path = os.path.join(dirpath, filename)
            df = pd.read_csv(path, encoding='utf-8-sig', parse_dates=['date'])
            df.index = df['date']
            col_name = filename[:-4]

            col_long = df['long_position_increase']
            col_long.name = col_name
            dfs_long = pd.concat([dfs_long, col_long], axis=1)

            col_short = df['short_position_increase']
            col_short.name = col_name
            dfs_short = pd.concat([dfs_short, col_short], axis=1)

        path2 = os.path.join(output_path, 'top20_long_position_increase.csv')
        dfs_long.to_csv(path2, encoding='utf-8-sig', date_format='%Y-%m-%d')

        path2 = os.path.join(output_path, 'top20_short_position_increase.csv')
        dfs_short.to_csv(path2, encoding='utf-8-sig', date_format='%Y-%m-%d')


if __name__ == '__main__':
    input_path = os.path.join(__CONFIG_H5_FUT_DATA_DIR__, "futureoir")
    output_path = os.path.join(__CONFIG_H5_FUT_DATA_DIR__, "futureoir_processed")

    member_name = '前二十名合计'
    folder_name = 'top20'
    process1(input_path, output_path, member_name, folder_name)

    input_path = os.path.join(__CONFIG_H5_FUT_DATA_DIR__, "futureoir_processed", "top20")
    output_path = os.path.join(__CONFIG_H5_FUT_DATA_DIR__, "futureoir_processed")
    process2(input_path, output_path)
