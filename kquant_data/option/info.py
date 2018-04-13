#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

"""
from ..xio.csv import read_data_dataframe


def read_optioncontractbasicinfo(path):
    """
    http://www.sse.com.cn/disclosure/announcement/general/c/c_20171127_4425064.shtml
    2017年11月28号除权日

    除权除息时行权价和合约单位都需要调整
    调整后合约行权价可能与之前重复，如2.75被调成2.7，而2.7被调成2.651
    可能挂的合约执行价相同

10001025	510050C1806A02700	50ETF购6月2.651A	10185	10/26/2017 0:00
10001026	510050C1806A02750	50ETF购6月2.70A	10185	10/26/2017 0:00
10001139	510050C1806M02700	50ETF购6月2.70	10000	12/18/2017 0:00
10001027	510050C1806A02800	50ETF购6月2.749A	10185	10/26/2017 0:00
10001131	510050C1806M02750	50ETF购6月2.75	10000	12/1/2017 0:00

    由于执行价重复，所以不同软件显示不同
    万得显示的是老合约，而通达信显示的是新合约
    :param path:
    :return:
    """
    df = read_data_dataframe(path)
    df_extract1 = df['trade_code'].str.extract('(\d{6})([CP])(\d{4})(\D)\d{5}', expand=True)
    df_extract1.columns = ['option_mark_code', 'call_or_put', 'limit_month', 'limit_month_m']
    df_extract1['limit_month'] = df_extract1['limit_month'].astype(int)
    # 这个以后再拼接也成
    # df_extract1['option_mark_code'] = df_extract1['option_mark_code'] + '.SH'

    # 已经退市sec_name会变，主要是年份的区别
    df_extract2 = df['sec_name'].str.split('月', expand=True)
    df_extract2[1] = df_extract2[1].str.extract('([\d\.-]*)', expand=True)
    df_extract2[1] = df_extract2[1].astype(float)
    df_extract2.columns = ['tmp', 'exercise_price']

    df = df.merge(df_extract1, how='left', left_index=True, right_index=True)
    df = df.merge(df_extract2[['exercise_price']], how='left', left_index=True, right_index=True)
    df.index = df.index.astype(str)
    return df
