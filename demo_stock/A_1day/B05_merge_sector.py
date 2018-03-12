#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
指定数据目录，生成对应的合约行业数据
"""
import sys
from kquant_data.processing.merge import merge_sector, merge_sectors

# 解决Python 3.6的pandas不支持中文路径的问题
print(sys.getfilesystemencoding())  # 查看修改前的
try:
    sys._enablelegacywindowsfsencoding()  # 修改
    print(sys.getfilesystemencoding())  # 查看修改后的
except:
    pass

if __name__ == '__main__':
    rule = '1day'
    sector_name = '风险警示股票'
    merge_sector(rule, sector_name, 'ST')

    sector_name = '中信证券一级行业指数'
    merge_sectors(rule, sector_name, 'Sector')

    print("Done")
    debug = 1
