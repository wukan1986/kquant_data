#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
指定数据目录，生成对应的合约行业数据
"""
from kquant_data.processing.merge import merge_weight

if __name__ == '__main__':
    rule = '1day'
    wind_code = '000300.SH'
    merge_weight(rule, wind_code, 'weight')
