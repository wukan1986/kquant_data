#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载合约信息
"""
from WindPy import w
from datetime import datetime
from kquant_data.wind.tdays import read_tdays
from kquant_data.wind.wss import download_ipo_last_trade_trading
from kquant_data.xio.csv import write_data_dataframe
from kquant_data.config import __CONFIG_H5_FUT_SECTOR_DIR__, __CONFIG_TDAYS_SSE_FILE__

if __name__ == '__main__':
    w.start()

    date_str = datetime.today().strftime('%Y-%m-%d')

    trading_days = read_tdays(__CONFIG_TDAYS_SSE_FILE__)
    trading_days = trading_days['2017-12-01':date_str]

    df = download_ipo_last_trade_trading(w, "IF1801.CFE,10000001.SH,I1801.DCE")

    df['wind_code'] = df.index
    df.sort_values(by=['ipo_date', 'wind_code'], inplace=True)
    del df['wind_code']

    write_data_dataframe(r'd:\9.csv', df)
