#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
每天获取数据，将数据合并成几个大的数据表
先取交易日，再取合约列表，然后全加载分成几个大表
"""
import gc
import multiprocessing
import os
import shutil
from functools import partial

import h5py
import numpy as np
import pandas as pd

from .utils import split_into_group, filter_dataframe
from ..utils.xdatetime import tic, toc
from ..xio.h5 import read_h5
from ..stock.symbol import get_symbols_from_path


class MergeBar(object):
    def __init__(self, folder):
        self.prefix = 'tmp'
        self.folder = folder
        self.datetime = None
        self.instruments = None
        self.instruments_group = None
        self.fields = None
        self.group_len = 1000
        # datetime与bar_size是相关联的
        self.bar_size = 86400
        self.init_datetime()
        self.init_symbols()
        self.init_fields()

    def init_datetime(self):
        path = os.path.join(self.folder, 'DateTime.csv')
        self.datetime.to_csv(path)

    def init_symbols(self):
        # 不再从导出列表中取，而是从文件夹中推算
        path = os.path.join(self.folder, 'sh')
        df_sh = get_symbols_from_path(path, "SSE")
        path = os.path.join(self.folder, 'sz')
        df_sz = get_symbols_from_path(path, "SZSE")
        df = pd.concat([df_sh, df_sz])

        self.instruments = df

        path = os.path.join(self.folder, 'Symbol.csv')
        self.instruments.to_csv(path, index=False)
        self.instruments_group = split_into_group(self.instruments, self.group_len)

    def init_fields(self):
        pass

    def read_data(self, market, code, bar_size):
        return None

    def _save_data(self, folder, raw_data, field):
        data = raw_data.astype(np.float64).as_matrix()
        path = os.path.join(folder, field + '.h5')
        file = h5py.File(path, 'w')
        file.create_dataset(field, data=data, compression="gzip", compression_opts=6)
        file.close()
        return None

    def _merge_data(self, datetime, instruments, i):
        t = instruments.iloc[i]
        print("%d %s" % (i, t['local_symbol']))
        df = self.read_data(t['market'], t['code'], self.bar_size)
        if df is None:
            return None
        del df['DateTime']
        df = pd.merge(df, datetime, left_index=True, right_index=True, how='right', copy=False)
        df = filter_dataframe(df, None, None, None, self.fields)

        return tuple(df.T.values)

    def _merge_branch(self, folder, datetime, instruments):
        multi = False
        if multi:
            pool_size = multiprocessing.cpu_count() - 1
            pool = multiprocessing.Pool(processes=pool_size)
            func = partial(self._merge_data, datetime, instruments)
            pool_outputs = pool.map(func, range(len(instruments)))
        else:
            pool_outputs = []
            for i in range(len(instruments)):
                x = self._merge_data(datetime, instruments, i)
                #if x is not None:
                pool_outputs.append(x)

        print("数据已经全部读取完成")
        toc()
        print("回收一下内存:%d" % gc.collect())
        # 其中可能有Nono的，需要处理成nan，不能丢弃，否则可能导致Symbol.csv对不上
        # pool_outputs
        pool_outputs = pd.Panel(pool_outputs)  # 内存不够，可能崩溃
        pool_outputs = pool_outputs.transpose(1, 2, 0)
        print("数据转置完成")
        toc()

        for i in range(len(self.fields)):
            print(self.fields[i])

            self._save_data(folder, pool_outputs.loc[i, :, :], self.fields[i])

        toc()
        print("回收一下内存:%d" % gc.collect())

    def merge(self):
        # 数据处理
        tic()

        # 想法对合约进行分组，分组后在对应目录下创建小文件，最后将小文件合并
        for index, item in enumerate(self.instruments_group):
            # 先创建目录
            sub_folder = os.path.join(self.folder, "%s_%d" % (self.prefix, index))
            os.makedirs(sub_folder, exist_ok=True)

            self._merge_branch(sub_folder, self.datetime, item)

        print("分批生成数据完成")
        toc()

    def hmerge(self):
        # 合并数据
        for i in range(len(self.fields)):
            field = self.fields[i]
            print(self.fields[i])
            data = None
            for index, item in enumerate(self.instruments_group):
                # 先创建目录
                sub_folder = os.path.join(self.folder, "%s_%d" % (self.prefix, index))
                sub_file = os.path.join(sub_folder, "%s.h5" % field)
                df = read_h5(sub_file, field)
                if data is None:
                    data = df
                else:
                    data = np.hstack((data, df))

            path = os.path.join(self.folder, field + '.h5')
            file = h5py.File(path, 'w')
            file.create_dataset(field, data=data, compression="gzip", compression_opts=6)
            file.close()

    def clear(self):
        instruments_group = split_into_group(self.instruments, self.group_len)

        # 想法对合约进行分组，分组后在对应目录下创建小文件，最后将小文件合并
        for index, item in enumerate(instruments_group):
            # 先创建目录
            sub_folder = os.path.join(self.folder, "%s_%d" % (self.prefix, index))
            shutil.rmtree(sub_folder)
            print(sub_folder)
        print("目录已删除")
