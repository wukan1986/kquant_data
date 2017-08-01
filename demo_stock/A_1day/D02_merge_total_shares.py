#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
合并财务数据
"""
from kquant_data.processing.merge import merge_report

if __name__ == '__main__':
    rule = '1day'
    field = 'total_shares'
    merge_report(rule, field, field)

    print("Done")
    debug = 1
