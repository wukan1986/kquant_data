#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
增量下载期货仓单数据
"""
from WindPy import w
import os
from datetime import datetime
from kquant_data.wind_resume.wsd import resume_download_daily_many_to_one_file
from kquant_data.future.symbol import get_all_products_wind
from kquant_data.config import __CONFIG_H5_FUT_DATA_DIR__

if __name__ == '__main__':
    w.start()

    date_str = datetime.today().strftime('%Y-%m-%d')

    windcodes = get_all_products_wind()

    root_path = os.path.join(__CONFIG_H5_FUT_DATA_DIR__, "st_stock")

    # 下载仓单数据
    df_old_output, df = resume_download_daily_many_to_one_file(w, windcodes, field='st_stock', dtype=None,
                                                               enddate=date_str,
                                                               root_path=root_path,
                                                               option="", default_start_date='2006-01-01')
