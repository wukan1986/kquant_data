#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据处理的小工具
"""
import numpy as np
import pandas as pd


def read_datetime_dataframe(path, sep=','):
    """
    读取季报的公告日，即里面全是日期
    注意：有些股票多个季度一起发，一般是公司出问题了，特别是600878，四个报告同一天发布
    年报与一季报很有可能一起发

    读取ipo_date
    :param path:
    :param sep:
    :return:
    """
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True, encoding='utf-8-sig', sep=sep)
    except (FileNotFoundError, OSError):
        return None

    df = df.stack(dropna=False)
    df = pd.to_datetime(df)
    df = df.unstack()

    return df


def write_datetime_dataframe(path, df, date_format='%Y-%m-%d'):
    """
    需要确保数据是datetime64,否则写入的格式不好,占用大
    :param path:
    :param df:
    :return:
    """
    is_datetime = df.dtypes[0].type == np.datetime64
    if not is_datetime:
        df = df.stack(dropna=False)
        df = pd.to_datetime(df)
        df = df.unstack()

    df.to_csv(path, date_format=date_format, encoding='utf-8-sig')


def read_data_dataframe(path, sep=','):
    """
    读取季报的公告日
    注意：有些股票多个季度一起发，一般是公司出问题了，特别是600878，四个报告同一天发布
    年报与一季报很有可能一起发
    :param path:
    :param sep:
    :return:
    """
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True, encoding='utf-8-sig', sep=sep)
    except (FileNotFoundError, OSError):
        return None

    return df


def write_data_dataframe(path, df, date_format='%Y-%m-%d'):
    """
    写入数据
    :param path:
    :param df:
    :return:
    """
    df.to_csv(path, date_format=date_format, encoding='utf-8-sig')
