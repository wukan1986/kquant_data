#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
得到合约的上市时间
有新品种上市时才运行
"""
import os
import numpy as np
from WindPy import w
from kquant_data.wind_resume.wsd import resume_download_delist_date
from kquant_data.future.symbol import get_all_products_wind, get_all_products_wind_S
from kquant_data.config import __CONFIG_H5_FUT_FACTOR_DIR__

if __name__ == '__main__':
    w.start()

    # 加载股票列表，这里需要在每天收盘后导出日线数据才能做
    wind_codes = get_all_products_wind()
    wind_codes_s = get_all_products_wind_S()
    wind_codes.extend(wind_codes_s)

    #
    if True:
        resume_download_delist_date(w, wind_codes, __CONFIG_H5_FUT_FACTOR_DIR__,
                                    field='contract_issuedate',
                                    dtype=np.datetime64)
