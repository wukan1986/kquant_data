#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
已经下载到了每个合约的最新主力信息
现在对数据进一步整理成表单
"""
import os

import pandas as pd
from kquant_data.config import __CONFIG_H5_FUT_FACTOR_DIR__
from kquant_data.xio.csv import read_data_dataframe, write_data_dataframe
from kquant_data.future.symbol import wind_code_2_InstrumentID

if __name__ == '__main__':
    input_path = os.path.join(__CONFIG_H5_FUT_FACTOR_DIR__, 'trade_hiscode')
    output_path = os.path.join(__CONFIG_H5_FUT_FACTOR_DIR__, 'trade_hiscode.csv')
    df_csv = pd.DataFrame(columns=['trade_hiscode'])
    for dirpath, dirnames, filenames in os.walk(input_path):
        for filename in filenames:
            shotname, extension = os.path.splitext(filename)
            dirpath_filename = os.path.join(dirpath, filename)
            print(dirpath_filename)
            _df = read_data_dataframe(dirpath_filename)
            _df.dropna(inplace=True)

            df_csv.loc[shotname] = None
            df_csv.loc[shotname]['trade_hiscode'] = _df.iat[-1, 0]

    df = wind_code_2_InstrumentID(df_csv, 'trade_hiscode')
    df.index.name = 'product'
    write_data_dataframe(output_path, df)
    print(output_path)
    # print(df)
    debug = 1
