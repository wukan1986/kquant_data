#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载期权合约信息
注意挂新合约的问题，没办法完全增量下载
只能第一次全部下载，然后以后每次下载正在交易的合约

w_wset_data = vba_wset("optioncontractbasicinfo","exchange=sse;windcode=510050.SH;status=trading;field=wind_code,trade_code,sec_name,contract_unit,listed_date,expire_date,reference_price",w_wset_codes,w_wset_fields,w_wset_times,w_wset_errorid)

"""
from WindPy import w
import os
import pandas as pd
from kquant_data.config import __CONFIG_H5_FUT_DIR__
from kquant_data.wind.wset import download_optioncontractbasicinfo
from kquant_data.xio.csv import write_data_dataframe, read_data_dataframe

from kquant_data.option.info import read_optioncontractbasicinfo


if __name__ == '__main__':
    # w.start()

    root_path = os.path.join(__CONFIG_H5_FUT_DIR__, 'optioncontractbasicinfo', '510050.SH.csv')

    # 这个只要执行一次就可以了，以后每次更新时都跳过即可
    if False:
        df_new = download_optioncontractbasicinfo(w, status='delisted')
        df_new = df_new.set_index('wind_code')
        write_data_dataframe(root_path, df_new)

    # 下载新数据，并合并上旧数据
    if False:
        df_old = read_data_dataframe(root_path)
        df_new = download_optioncontractbasicinfo(w, status='trading')
        df_new = df_new.set_index('wind_code')

        df = pd.concat([df_old, df_new])
        #
        df = df[~df.index.duplicated(keep='last')]
        # df.drop_duplicates(keep='last', inplace=True)
        write_data_dataframe(root_path, df)

    df = read_optioncontractbasicinfo(root_path)

    debug = 1
