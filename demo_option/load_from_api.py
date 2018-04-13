#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
读取数据示例
"""
import os
from kquant_data.config import __CONFIG_H5_FUT_DIR__
from kquant_data.api import get_price
from kquant_data.option.info import read_optioncontractbasicinfo


get_price('sh')

root_path = os.path.join(__CONFIG_H5_FUT_DIR__, 'optioncontractbasicinfo', '510050.SH.csv')
df_info = read_optioncontractbasicinfo(root_path)
# 排序一下，方便显示
df_info = df_info.sort_values(by=['limit_month', 'call_or_put', 'exercise_price'])
df_info = df_info.reset_index()
# df_info = df_info.set_index(['limit_month', 'call_or_put', 'exercise_price', 'limit_month_m'])
df_info = df_info.set_index(['limit_month', 'call_or_put', 'exercise_price'])
# df_info.to_csv(r'd:\x.csv', encoding='utf-8-sig')

# 选择具体的到期月份，这下可以画T型报价图了
df_filtered = df_info[df_info.index.get_level_values('limit_month') == 1806]

df = get_price(list(df_filtered['wind_code']), path=r'D:\shqqday', instrument_type='option',
               new_symbols=list(df_filtered['trade_code']))
print(df)

# 选择每天收盘时的平值期权
