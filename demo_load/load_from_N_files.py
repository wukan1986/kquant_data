#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
读取数据示例
"""
import os
import numpy as np

from kquant_data.api import get_datetime, all_instruments
from kquant_data.xio.h5 import read_h5
from kquant_data.processing.utils import ndarray_to_dataframe

"""
基础数据准备
"""
start_date = '2007'  # 测试开始时间
end_date = None  # 结束时间
input_path = r'D:\DATA_STK\daily'  # 指定输入数据目录
output_path = 'tmp_data'  # 指定输入数据目录

path = os.path.join(input_path, 'DateTime.csv')
DateTime = get_datetime(path)

path = os.path.join(input_path, 'Symbol.csv')
df_Symbols = all_instruments(path)
Symbols = df_Symbols['wind_code']

"""
行情数据准备
"""
# 一定要复权，但需要选择好复权的时机

path = os.path.join(input_path, 'Close.h5')
Close = read_h5(path, 'Close')
Close = ndarray_to_dataframe(Close, DateTime.index, columns=Symbols, start=start_date, end=end_date)
Close.replace(0, np.nan, inplace=True)  # 可能出现价格为0的错误数据，需要设成nan等待抛弃

path = os.path.join(input_path, 'Open.h5')
Open = read_h5(path, 'Open')
Open = ndarray_to_dataframe(Open, DateTime.index, columns=Symbols, start=start_date, end=end_date)


path = os.path.join(input_path, 'forward_factor.h5')
forward_factor = read_h5(path, 'forward_factor')
forward_factor = ndarray_to_dataframe(forward_factor, DateTime.index, columns=Symbols, start=start_date, end=end_date)

"""
交易价格
"""
pricing = Open * forward_factor


print(Close.head())
print(pricing.tail())
