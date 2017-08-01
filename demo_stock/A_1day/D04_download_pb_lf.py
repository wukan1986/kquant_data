#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载财报一类的信息
"""
import os
import pandas as pd
from WindPy import w
from kquant_data.xio.csv import write_data_dataframe, read_data_dataframe
from kquant_data.api import all_instruments
from kquant_data.wind_resume.wsd import download_daily_at

from kquant_data.config import __CONFIG_H5_STK_FACTOR_DIR__, __CONFIG_H5_STK_DIR__

if __name__ == '__main__':
    w.start()

    path = os.path.join(__CONFIG_H5_STK_DIR__, '1day', 'Symbol.csv')
    Symbols = all_instruments(path)
    wind_codes = list(Symbols['wind_code'])

    # 下载多天数据,以另一数据做为标准来下载
    # 比如交易数据是10月8号，那就得取10月7号，然后再平移到8号，如果7号没有数据那就得9月30号
    path = os.path.join(__CONFIG_H5_STK_FACTOR_DIR__, 'roe.csv')
    date_index = read_data_dataframe(path)

    field = 'pb_lf'
    df = None
    for i in range(len(date_index)):
        print(date_index.index[i])

        date_str = date_index.index[i].strftime('%Y-%m-%d')
        df_new = download_daily_at(w, wind_codes, field, date_str, "Days=Alldays")
        if df is None:
            df = df_new
        else:
            df = pd.concat([df, df_new])

    path = os.path.join(__CONFIG_H5_STK_FACTOR_DIR__, '%s.csv' % field)
    write_data_dataframe(path, df)
