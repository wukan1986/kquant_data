#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日数据和权息数全都导出
用户直接读取h5格式的数据即可
"""
import os
import pandas as pd

from kquant_data.config import __CONFIG_DZH_PWR_FILE__, __CONFIG_H5_STK_DIVIDEND_DIR__, __CONFIG_TDX_STK_DIR__, \
    __CONFIG_H5_STK_DIR__
from kquant_data.stock.stock import export_dividend_daily_gbbq
from kquant_data.stock.symbol import get_symbols_from_path_only_stock


def export_daily():
    """
    只导出有除权信息的股票，没有导出的数据，以后直接读通达信
    :return:
    """
    # 复权因子的导出
    dividend_input = os.path.join(__CONFIG_H5_STK_DIR__, 'gbbq.csv')
    dividend_output = __CONFIG_H5_STK_DIVIDEND_DIR__
    daily_input = os.path.join(__CONFIG_TDX_STK_DIR__, "vipdoc")
    daily_output = os.path.join(__CONFIG_H5_STK_DIR__, "1day")
    # export_dividend_daily(dividend_input, daily_input, dividend_output, daily_output)
    export_dividend_daily_gbbq(dividend_input, daily_input, dividend_output, daily_output)


def export_symbols():
    path = os.path.join(__CONFIG_TDX_STK_DIR__, "vipdoc", 'sh', 'lday')
    df_sh = get_symbols_from_path_only_stock(path, "SSE")
    path = os.path.join(__CONFIG_TDX_STK_DIR__, "vipdoc", 'sz', 'lday')
    df_sz = get_symbols_from_path_only_stock(path, "SZSE")
    df = pd.concat([df_sh, df_sz])

    return df


if __name__ == '__main__':
    # 导出合约信息，要有合约的名字
    list = export_symbols()
    # 这个保存相当重要，因为日线数据没有除权信息的就不转换了
    path = os.path.join(__CONFIG_H5_STK_DIR__, "1day", 'Symbol.csv')
    list.to_csv(path, index=False)

    # 导出股票日线
    export_daily()

    # 下断点用
    debug = 1
