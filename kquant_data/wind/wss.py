#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
调用wss函数的部分
"""
from WindPy import w
import os
import pandas as pd
from .utils import asDateTime

from ..utils.xdatetime import datetime_2_yyyy
from ..utils.xdatetime import datetime_2_yyyyMMdd


def download_fellow_listeddate(w, wind_codes, year):
    """
    增发上市日
    本想通过它来确认股本，但实际发现还是有一些差别，可能需要补充多下一些数据
    :param w:
    :param wind_codes:
    :param year:
    :return:
    """
    w.asDateTime = asDateTime
    w_wss_data = w.wss(wind_codes, "fellow_listeddate", "year=%s" % year)
    df = pd.DataFrame(w_wss_data.Data)
    df = df.T
    df.columns = ['fellow_listeddate']
    df.index = w_wss_data.Codes
    return df


def download_ipo_last_trade_trading(w, wind_codes):
    # 黄金从20130625开始将最小变动价位从0.01调整成了0.05，但从万得上查出来还是完全一样，所以没有必要记录mfprice
    # 郑商所在修改合约交易单位时都改了合约代码，所以没有必要记录contractmultiplier
    w.asDateTime = asDateTime
    # w_wss_data = w.wss(wind_codes, "sec_name,ipo_date,lasttrade_date,lasttradingdate,contractmultiplier,mfprice", "")
    w_wss_data = w.wss(wind_codes, "sec_name,ipo_date,lasttrade_date,lasttradingdate", "")
    df = pd.DataFrame(w_wss_data.Data)
    df = df.T
    # df.columns = ['sec_name', 'ipo_date', 'lasttrade_date', 'lasttradingdate', 'contractmultiplier', 'mfprice']
    df.columns = ['sec_name', 'ipo_date', 'lasttrade_date', 'lasttradingdate']
    df.index = w_wss_data.Codes
    df.index.name = 'wind_code'

    df['ipo_date'] = df['ipo_date'].apply(datetime_2_yyyyMMdd)
    df['lasttrade_date'] = df['lasttrade_date'].apply(datetime_2_yyyyMMdd)
    df['lasttradingdate'] = df['lasttradingdate'].apply(datetime_2_yyyyMMdd)
    # df['contractmultiplier'] = df['contractmultiplier'].astype(int)

    df.replace(18991230, 0, inplace=True)

    return df


def download_test(w):
    """
    有问题，现在没法用
    :param w:
    :return:
    """
    df = pd.read_csv(r'D:\DATA_STK_HDF5\factor\seoimplementation.csv', parse_dates=True, index_col=0,
                     dtype={'issue_code': str})
    df['seo_anncedate'] = pd.to_datetime(df['seo_anncedate'])

    df['yyyy'] = df['seo_anncedate'].apply(datetime_2_yyyy)
    dfg = df.groupby('yyyy')
    for name, group in dfg:
        if name < 2007 or name > 2017:
            continue
        # 不管那么多，重复的就记下来
        print(name)
        group_counts = group['wind_code'].value_counts()
        group_counts = pd.DataFrame(group_counts)
        xy = pd.merge(group, group_counts, left_on='wind_code', right_index=True)

        print(group_counts)

        # 这里有问题，由于部分股票会出现两次增发，还隔很近，返回的数据只有一条
        # 放弃通过增发的方式来判断股票股本变动
        # 所以也同样不通过分红的方式
        dfx = download_fellow_listeddate(w, group['wind_code'].tolist(), name)
        dfx['fellow_listeddate'] = pd.to_datetime(dfx['fellow_listeddate'])
        path = os.path.join(r'D:\DATA_STK_HDF5\factor\fellow_listeddate', "%s.csv" % name)
        dfx.to_csv(path, date_format='%Y-%m-%d', encoding='utf-8-sig')


if __name__ == '__main__':
    w.start()
    download_test(w)

    debug = 1
