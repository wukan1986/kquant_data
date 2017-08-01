#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载除权文件
"""
from kquant_data.config import __CONFIG_DZH_PWR_FILE__
from kquant_data.stock.dzh import download_pwr

if __name__ == '__main__':
    download_pwr(__CONFIG_DZH_PWR_FILE__)
