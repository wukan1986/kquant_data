#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
全市场计算权重
"""

from kquant_data.processing.merge import merge_weight


def merge_weight_000300():
    rule = '1day'
    wind_code = '000300.SH'
    merge_weight(rule, wind_code, 'weight')


def merge_weight_000016():
    rule = '1day'
    wind_code = '000016.SH'
    merge_weight(rule, wind_code, 'weight')


if __name__ == '__main__':
    merge_weight_000300()
