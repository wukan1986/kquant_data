#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载财报一类的信息
"""
import os
import numpy as np
from WindPy import w
from kquant_data.api import all_instruments
from kquant_data.wind_resume.wsd import resume_download_financial_report_quarter

from kquant_data.config import __CONFIG_H5_STK_DIR__, __CONFIG_H5_STK_FACTOR_DIR__

if __name__ == '__main__':
    w.start()

    path = os.path.join(__CONFIG_H5_STK_DIR__, '1day', 'Symbol.csv')
    Symbols = all_instruments(path)
    wind_codes = list(Symbols['wind_code'])

    # 下载时间类型的数据
    fields = ['stm_issuingdate']
    if True:
        resume_download_financial_report_quarter(w, wind_codes, __CONFIG_H5_STK_FACTOR_DIR__, fields=fields,
                                                 dtype=np.datetime64)

    # 下载一般类型的数据，只在季报中出现，平时不出现的数据，如roe，pb_lf由于每天变化不通过这种方式下载
    fields = ['close', 'cap_stk', 'np_belongto_parcomsh', 'eqy_belongto_parcomsh']
    fields = ['ebit', 'ebit2', 'networkingcapital', 'fix_assets', 'ev', 'mkt_cap_ard', 'interestdebt']
    fields = ['wgsd_net_inc', 'oper_rev', 'wgsd_oper_cf', 'wgsd_liabs_lt', 'monetary_cap', 'tot_assets']
    fields = ['np_belongto_parcomsh', 'eqy_belongto_parcomsh']
    fields = ['close']
    fields = ['np_belongto_parcomsh']
    fields = ['pb_lf']
    fields = ['roe']
    if False:
        resume_download_financial_report_quarter(w, wind_codes, __CONFIG_H5_STK_FACTOR_DIR__, fields=fields, dtype=None)
