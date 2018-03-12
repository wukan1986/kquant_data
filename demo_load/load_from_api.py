#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
读取数据示例
"""
from kquant_data.api import get_price


pricing = get_price('600000.SH')
print(pricing)
pricing = get_price(['600000.SH', '000001.SZ'])
print(pricing)
pricing = get_price(['000001.SZ'], start_date='2010-01-01')
print(pricing)
pricing = get_price(['000001.SZ'], start_date='2010-01-01', bar_size=300, fields=['Close'])
print(pricing)
pricing = get_price(['000001.SZ'], start_date='2010-01-01', bar_size=86400)
print(pricing)

# 拿到的价格是原始价格，需要自己复权

# 向前复权
pricing['Close_forward_factor'] = pricing['Close'] * pricing['forward_factor']
print(pricing.tail())
# 向后复权
pricing['Close_backward_factor'] = pricing['Close'] * pricing['backward_factor']
print(pricing)

pricing = get_price(['IF.'], start_date='2010-01-01', bar_size=86400)
print(pricing)
