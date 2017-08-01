#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
将期货数据合并，成与股票同风格
每个字段放一个文件，同时股票处理也快
"""
import os
import pandas as pd

from kquant_data.config import __CONFIG_H5_STK_DIR__
from kquant_data.processing.utils import filter_dataframe
from kquant_data.processing.MergeBar import MergeBar


class MergeDataStock_5min(MergeBar):
    def __init__(self, folder):
        super(MergeDataStock_5min, self).__init__(folder)
        self.bar_size = 3600

    def init_datetime(self):
        # 从600000这种时间取数据
        h5_path = os.path.join(self.folder, 'sh', 'sh600000.h5')
        df = pd.read_hdf(h5_path)
        df = filter_dataframe(df, 'DateTime', None, None, fields=['DateTime'])
        # 可以保存，也可以不保存
        self.datetime = df
        super(MergeDataStock_5min, self).init_datetime()


    def init_fields(self):
        self.fields = ['Open', 'High', 'Low', 'Close', 'Volume', 'Amount', 'backward_factor', 'forward_factor']

    def read_data(self, market, code, bar_size):
        h5_path = os.path.join(self.folder, market, '%s%s.h5' % (market, code))
        df = pd.read_hdf(h5_path)
        df = filter_dataframe(df, 'DateTime', None, None, None)
        return df


if __name__ == '__main__':
    # 导出日线数据
    path = os.path.join(__CONFIG_H5_STK_DIR__, "1hour")

    mdf = MergeDataStock_5min(path)
    mdf.merge()
    mdf.hmerge()
    mdf.clear()
