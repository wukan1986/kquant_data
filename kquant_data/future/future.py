#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
期货的处理方法
"""
import os

import pandas as pd

from ..config import __CONFIG_H5_FUT_MARKET_DATA_DIR__


def bar_size_2_folder(bar_size):
    ret = {
        86400: '86400_DEF1_MC1',
        3600: '3600_DEF1_MC1_1530_EXT',
    }[bar_size]
    return ret


def get_relative_path(market, code, bar_size):
    # D:\DATA_FUT_HDF5\Data_Processed\86400_DEF1_MC1\a.h5
    folder = bar_size_2_folder(bar_size)
    file_ext = 'h5'
    filename = "%s.%s" % (code, file_ext)
    return os.path.join(folder, filename)


def get_absolute_path(root_dir, market, code, bar_size):
    return os.path.join(root_dir, get_relative_path(market, code, bar_size))


def read_future(market, code, bar_size, path):
    if path is None:
        _path = get_absolute_path(__CONFIG_H5_FUT_MARKET_DATA_DIR__, market, code, bar_size)
    else:
        file_ext = 'h5'
        filename = "%s.%s" % (code, file_ext)
        _path = os.path.join(path, filename)
    df = pd.read_hdf(_path)
    return df
