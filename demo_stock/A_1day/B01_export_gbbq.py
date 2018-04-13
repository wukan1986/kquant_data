#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
导出通达信的股本变迁
"""
import os
from kquant_data.config import __CONFIG_TDX_GBBQ_FILE__, __CONFIG_H5_STK_DIR__
from kquant_data.stock.gbbq import GbbqReader

if __name__ == '__main__':
    result = GbbqReader().get_df(__CONFIG_TDX_GBBQ_FILE__)
    path = os.path.join(__CONFIG_H5_STK_DIR__, 'gbbq.csv')
    result.to_csv(path)
    # print(result)
    print("除权信息导出成功")
