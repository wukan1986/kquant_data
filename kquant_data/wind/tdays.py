#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

w.tdays("2017-02-02", "2017-03-02", "")
w.tdays("2017-02-02", "2017-03-02", "Days=Weekdays")
w.tdays("2017-02-02", "2017-03-02", "Days=Alldays")
w.tdays("2017-02-02", "2017-03-02", "TradingCalendar=SHFE")
"""
import pandas as pd
from .utils import asDateTime


def download_tdays(w, startdate, enddate, option=''):
    """
    下载交易日数据
    :param w:
    :param startdate:
    :param enddate:
    :param option:
    :return:
    """
    w.asDateTime = asDateTime
    w_tdays_data = w.tdays(startdate, enddate, option)
    df = pd.DataFrame(w_tdays_data.Data, )
    df = df.T
    df.columns = ['date']
    df['date'] = pd.to_datetime(df['date'])

    return df


def read_tdays(path):
    try:
        df = pd.read_csv(path, parse_dates=True)
    except:
        return None

    df['date'] = pd.to_datetime(df['date'])
    df.index = df['date']
    return df


def write_tdays(path, df):
    df.to_csv(path, date_format='%Y-%m-%d', encoding='utf-8', index=False)
