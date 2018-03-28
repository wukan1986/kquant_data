#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
调用wset函数的部分

单位累计分红可以间接拿到分红日期
w.wsd("510050.SH", "div_accumulatedperunit", "2016-01-25", "2017-02-23", "")

"""
from ..processing.utils import *
from .utils import asDateTime


def download_min_ohlcv(w, wind_code, start, end):
    """
    下载分钟数据
    :param w:
    :param wind_code:
    :param start:
    :param end:
    :param exchange:
    :return:
    """
    # 在分数据数上不能改写时间函数，否则返回的时间有错
    # w.asDateTime = asDateTime
    fields = 'open,high,low,close,volume,amt,oi'
    w_wsd_data = w.wsi(wind_code, fields, start, end, '')
    df = pd.DataFrame(w_wsd_data.Data, )
    df = df.T
    df.columns = w_wsd_data.Fields
    df.index = w_wsd_data.Times
    return df, w_wsd_data.Codes[0]
