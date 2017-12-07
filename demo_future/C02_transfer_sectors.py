#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
部分转码代码，只测试用，
"""
from WindPy import w
import pandas as pd
import os
from kquant_data.wind.wset import write_constituent

if __name__ == '__main__':
    path = r'D:\DATA_FUT\sectorconstituent\CHN_FUT\19950417.csv'
    path = r'D:\DATA_FUT\sectorconstituent\CHN_FUT\\'
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            # print(filename)
            new_filename = "%s-%s-%s.csv" % (filename[0:4], filename[4:6], filename[6:8])
            print(new_filename)
            new_path = os.path.join(dirpath, filename)

            df = pd.read_csv(new_path, encoding='gbk', parse_dates=True)
            # 期权可能名字也很重要，但如果在多个文件中都出现这个名字又太麻烦，最好还是使用别的表来映射名字
            # df = df[['wind_code', 'sec_name']]
            df = df[['wind_code']]

            df_SHF = df.loc[df['wind_code'].str.endswith('.SHF')]
            df_CFE = df.loc[df['wind_code'].str.endswith('.CFE')]
            df_DCE = df.loc[df['wind_code'].str.endswith('.DCE')]
            df_CZC = df.loc[df['wind_code'].str.endswith('.CZC')]

            path_SHF = r'D:\DATA_FUT\sectorconstituent\上期所全部品种\%s' % new_filename
            path_CFE = r'D:\DATA_FUT\sectorconstituent\中金所全部品种\%s' % new_filename
            path_DCE = r'D:\DATA_FUT\sectorconstituent\大商所全部品种\%s' % new_filename
            path_CZC = r'D:\DATA_FUT\sectorconstituent\郑商所全部品种\%s' % new_filename
            if len(df_SHF) > 0:
                write_constituent(path_SHF, df_SHF)
            if len(df_CFE) > 0:
                write_constituent(path_CFE, df_CFE)
            if len(df_DCE) > 0:
                write_constituent(path_DCE, df_DCE)
            if len(df_CZC) > 0:
                write_constituent(path_CZC, df_CZC)
