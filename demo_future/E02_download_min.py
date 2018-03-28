#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载合约信息
"""
from WindPy import w
import os
import pandas as pd
from kquant_data.wind.wss import download_ipo_last_trade_trading
from kquant_data.xio.csv import write_data_dataframe, read_data_dataframe
from kquant_data.config import __CONFIG_H5_FUT_SECTOR_DIR__
from kquant_data.wind.wset import read_constituent
from kquant_data.wind.wsi import download_min_ohlcv

if __name__ == '__main__':
    w.start()

    df, wind_code = download_min_ohlcv(w, 'IF1804.CFE', '2018-03-05 20:00:00', '2018-03-06 16:00:00')
    df.to_csv(r'd:\_IF1804.CFE.csv')
