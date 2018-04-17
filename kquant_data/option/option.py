#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
期货的处理方法
"""
import os

import pandas as pd

from ..config import __CONFIG_H5_FUT_MARKET_DATA_DIR__
from ..stock.tdx import read_file


def read_option(market, code, bar_size, path):
    if path is None:
        # _path = get_absolute_path(__CONFIG_H5_FUT_MARKET_DATA_DIR__, market, code, bar_size)
        pass
    else:
        file_ext = 'day'
        filename = "8#%s.%s" % (code, file_ext)
        _path = os.path.join(path, filename)
    df = read_file(_path, 'option')
    return df
