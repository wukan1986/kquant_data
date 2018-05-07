#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
指定数据目录，生成对应的合约行业数据

分为两种
1. 全市场数据，将部分标记上权重
2. 只对历史上成为成份股的，进行处理，由于前面已经转换了数据，这里只要跳选数据并处理即可

"""
import os
import pandas as pd
from kquant_data.config import __CONFIG_H5_STK_WEIGHT_DIR__, __CONFIG_H5_STK_DIR__, __CONFIG_TDX_STK_DIR__, \
    __CONFIG_H5_STK_DIVIDEND_DIR__
from kquant_data.processing.merge import merge_weight, load_index_weight, merge_weight_internal
from kquant_data.stock.symbol import get_symbols_from_wind_code_df
from kquant_data.api import get_datetime
from kquant_data.processing.utils import filter_dataframe, split_into_group
from kquant_data.processing.MergeBar import MergeBar
from kquant_data.stock.stock import read_h5_tdx


class MergeDataStock_5min_000300(MergeBar):
    def __init__(self, folder):
        super(MergeDataStock_5min_000300, self).__init__(folder)
        self.bar_size = 300

    def init_symbols(self):
        # 不再从导出列表中取，而是从文件夹中推算
        wind_code = '000300.SH'
        path = os.path.join(__CONFIG_H5_STK_WEIGHT_DIR__, wind_code)
        df = load_index_weight(path)
        wind_codes = pd.DataFrame(list(df.columns), columns=['wind_code'])
        df = get_symbols_from_wind_code_df(wind_codes)

        self.instruments = df

        path = os.path.join(self.folder, 'Symbol.csv')
        self.instruments.to_csv(path, index=False)
        self.instruments_group = split_into_group(self.instruments, self.group_len)

    def init_datetime(self):
        df = read_h5_tdx("sh", "000300", 300, __CONFIG_H5_STK_DIR__, __CONFIG_TDX_STK_DIR__,
                         __CONFIG_H5_STK_DIVIDEND_DIR__)

        df = filter_dataframe(df, 'DateTime', None, None, fields=['DateTime'])
        # 可以保存，也可以不保存
        self.datetime = df
        super(MergeDataStock_5min_000300, self).init_datetime()

    def init_fields(self):
        self.fields = ['Open', 'High', 'Low', 'Close', 'Volume', 'Amount', 'backward_factor', 'forward_factor']

    def read_data(self, market, code, bar_size):
        h5_path = os.path.join(__CONFIG_H5_STK_DIR__, '5min', market, '%s%s.h5' % (market, code))
        try:
            df = pd.read_hdf(h5_path)
            df = filter_dataframe(df, 'DateTime', None, None, None)
        except:
            return pd.DataFrame(columns=self.fields + ['DateTime'])
        return df


if __name__ == '__main__':
    # 得到50成份股内的各种开高低收等行情
    path = os.path.join(__CONFIG_H5_STK_DIR__, "5min_000300.SH")

    mdf = MergeDataStock_5min_000300(path)
    mdf.merge()
    mdf.hmerge()
    mdf.clear()

    pass
