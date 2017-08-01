#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
将期货数据合并，成与股票同风格
每个字段放一个文件，同时股票处理也快
"""
import os

from kquant_data.config import __CONFIG_H5_STK_DIR__, __CONFIG_TDX_STK_DIR__, __CONFIG_H5_STK_DIVIDEND_DIR__
from kquant_data.processing.utils import filter_dataframe
from kquant_data.stock.stock import read_h5_tdx
from kquant_data.processing.MergeBar import MergeBar


class MergeDataStock(MergeBar):
    def init_datetime(self):
        df = read_h5_tdx('sh', '000001', self.bar_size, __CONFIG_H5_STK_DIR__, __CONFIG_TDX_STK_DIR__,
                         __CONFIG_H5_STK_DIVIDEND_DIR__)
        # 少了8年的数据就不崩溃了，但这种方法肯定有问题
        df = filter_dataframe(df, 'DateTime', "2005", None, fields=['DateTime'])
        # 可以保存，也可以不保存
        self.datetime = df
        super(MergeDataStock, self).init_datetime()

    def init_fields(self):
        self.fields = ['Open', 'High', 'Low', 'Close', 'Volume', 'Amount', 'backward_factor', 'forward_factor']

    def read_data(self, market, code, bar_size):
        df = read_h5_tdx(market, code, self.bar_size, __CONFIG_H5_STK_DIR__, __CONFIG_TDX_STK_DIR__,
                         __CONFIG_H5_STK_DIVIDEND_DIR__)
        return df


if __name__ == '__main__':
    # 导出日线数据
    path = os.path.join(__CONFIG_H5_STK_DIR__, "1day")

    mdf = MergeDataStock(path)
    mdf.merge()
    mdf.hmerge()
    mdf.clear()
