#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据的保存
"""
import h5py
import numpy as np
import pandas as pd


def write_dataframe_set_struct_keep_head(path, data, dtype, dateset_name):
    """
    保存DataFrame数据
    保留表头
    可以用来存K线，除权除息等信息
    :param path:
    :param data:
    :param dtype:
    :param dateset_name:
    :return:
    """
    f = h5py.File(path, 'w')

    r = data.to_records(index=False)
    d = np.array(r, dtype=dtype)

    f.create_dataset(dateset_name, data=d, compression="gzip", compression_opts=6)
    f.close()
    return


def write_dataframe_set_dtype_remove_head(path, data, dtype, dataset_name):
    """
    每个单元格的数据类型都一样
    强行指定类型可以让文件的占用更小
    表头不保存
    :param path:
    :param data:
    :param dtype:
    :param dateset_name:
    :return:
    """
    f = h5py.File(path, 'w')
    if dtype is None:
        f.create_dataset(dataset_name, data=data.as_matrix(), compression="gzip", compression_opts=6)
    else:
        f.create_dataset(dataset_name, data=data, compression="gzip", compression_opts=6, dtype=dtype)
    f.close()
    return


def read_h5(path, dateset_name):
    """
    将简单数据读取出来
    返回的东西有头表，就是DataFrame，没表头就是array
    :param path:
    :param dateset_name:
    :return:
    """
    f = h5py.File(path, 'r')

    d = f[dateset_name][:]

    f.close()
    return d


def write_dataframe_with_index_columns(path, df, values_dtype, datetime_func):
    """
    保存dataframe列表，分成三部分保存，时间，列表头，数据
    因为时间轴可能与默认的不一样，所以使用这种方法在一个文件中保存一张表
    :param path:
    :param df:
    :param values_dtype:
    :param columns_dtype:
    :return:
    """
    f = h5py.File(path, 'w')

    f.create_dataset('values', data=df.as_matrix(), compression="gzip", compression_opts=6, dtype=values_dtype)

    index = df.index.map(datetime_func)
    f.create_dataset('index', data=index, compression="gzip", compression_opts=6, dtype=np.int64)

    # # 只能存定长byte类型的，变长的会导致工具打开出错
    columns = list(df.columns.map(lambda x: np.string_(x)))
    # f.create_dataset('columns', data=columns, dtype=columns_dtype)

    tid = h5py.h5t.C_S1.copy()
    tid.set_size(32)
    H5T_C_S1_32 = h5py.Datatype(tid)
    f.create_dataset('columns', data=columns, compression="gzip", compression_opts=6, dtype=H5T_C_S1_32)

    f.close()

    return


def read_dataframe_with_index_columns(path, datetime_func):
    """
    将表头数据和索引数据一起
    :param path:
    :return:
    """
    f = h5py.File(path, 'r')

    values = f['values'][:]
    index = f['index'][:]
    columns = f['columns'][:]
    columns = columns.astype(str)

    df = pd.DataFrame(values, index=index, columns=columns)
    df.index = df.index.map(datetime_func)

    f.close()

    return df
