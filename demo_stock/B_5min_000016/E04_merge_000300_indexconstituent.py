#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
指定数据目录，生成对应的合约行业数据

分为两种
1. 全市场数据，将部分标记上权重
2. 只对历史上成为成份股的，进行处理，由于前面已经转换了数据，这里只要跳选数据并处理即可


以前的做法是先生成数据，然后再生成合约
"""
import os
from kquant_data.config import __CONFIG_H5_STK_DIR__
from kquant_data.processing.merge import merge_weight_internal
from kquant_data.api import get_datetime, all_instruments
from kquant_data.xio.h5 import write_dataframe_set_dtype_remove_head

if __name__ == '__main__':
    # 时间和合约都已经生成了
    # 只要将时间与合约对上即可
    path = os.path.join(__CONFIG_H5_STK_DIR__, "5min_000300.SH", 'Symbol.csv')
    symbols = all_instruments(path)

    path = os.path.join(__CONFIG_H5_STK_DIR__, "5min_000300.SH", 'DateTime.csv')
    DateTime = get_datetime(path)

    df = merge_weight_internal(symbols, DateTime, "000300.SH")

    path = os.path.join(__CONFIG_H5_STK_DIR__, "5min_000300.SH", 'weight.h5')
    write_dataframe_set_dtype_remove_head(path, df, None, "weight")
