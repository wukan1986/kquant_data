#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
调用wset函数的部分

下载数据的方法
1.在时间上使用折半可以最少的下载数据，但已经下了一部分，要补下时如果挪了一位，又得全重下
2.在文件上，三个文件一组，三组一样，删中间一个，直到不能删了，退出
"""
import os
import pandas as pd
from .utils import asDateTime


def download_sectorconstituent(w, date, sector, windcode, field='wind_code'):
    """
    板块成份
    中信证券一级行业指数：时间好像没有必要，因为日历日也会查询出来
    风险警示股票：日期就是查询的日期，股票也是最新名，没有啥用

    w.wset("sectorconstituent","date=2017-03-03;sectorid=a001010100000000;field=wind_code")
    w.wset("sectorconstituent","date=2017-03-03;sectorid=a001010100000000")
    w.wset("sectorconstituent","date=2017-03-03;windcode=000300.SH")
    :param w:
    :param sector:
    :param date:
    :return:
    """
    param = 'date=%s' % date
    if sector:
        param += ';sector=%s' % sector
    if windcode:
        param += ';windcode=%s' % windcode
    if field:
        param += ';field=%s' % field

    w.asDateTime = asDateTime
    w_wset_data = w.wset("sectorconstituent", param)
    df = pd.DataFrame(w_wset_data.Data)
    df = df.T
    df.columns = w_wset_data.Fields
    try:
        df['date'] = pd.to_datetime(df['date'])
    except KeyError:
        pass
    return df


def read_sectorconstituent(path):
    """
    读取板块文件
    :param path:
    :return:
    """
    try:
        df = pd.read_csv(path, encoding='utf-8-sig', parse_dates=True)
    except (FileNotFoundError, OSError):
        return None
    try:
        df['date'] = pd.to_datetime(df['date'])
    except KeyError:
        pass
    return df


def read_sectorconstituent_from_dir(path, key_field='wind_code'):
    """
    从目录中读取整个文件
    :param path:
    :param key_field:
    :return:
    """
    last_set = None
    df = None
    for parent, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(parent, filename)
            curr_df = read_sectorconstituent(filepath)
            # 由于两头数据可能一样，这样处理，只留第一个，可以加快处理速度
            curr_set = set(curr_df[key_field])
            if last_set == curr_set:
                last_set = curr_set
                continue
            last_set = curr_set

            data_date_str = filename[:-4]
            curr_df['_datetime_'] = pd.to_datetime(data_date_str)
            if df is None:
                df = curr_df
            else:
                df = pd.concat([df, curr_df])
    return df


def write_sectorconstituent(path, df):
    df.to_csv(path, encoding='utf-8-sig', date_format='%Y-%m-%d', index=False)


def download_indexconstituent(w, date, windcode, field):
    """
    指数权重
    如果指定日期不是交易日，会返回时前一个交易日的信息
    :param w:
    :param windcode:
    :param date:
    :return:
    """
    param = 'date=%s' % date
    if windcode:
        param += ';windcode=%s' % windcode
    if field:
        param += ';field=%s' % field

    w.asDateTime = asDateTime
    w_wset_data = w.wset("indexconstituent", param)
    df = pd.DataFrame(w_wset_data.Data)
    df = df.T
    df.columns = w_wset_data.Fields
    return df


def download_corporationaction(w, startdate, enddate, windcode):
    """
    分红送转
    如何获取某一天的分红情况，开始与结束设成同一天，不设置wind_code
    实际上只取一个如600000.SH发现很早以前的信息可能缺失
    > w.wset('CorporationAction','startdate=20150605;enddate=20150605')
    :param w:
    :param windcode:
    :param date:
    :return:
    """
    param = 'startdate=%s' % startdate
    if enddate:
        param += ';enddate=%s' % enddate
    if windcode:
        param += ';windcode=%s' % windcode

    w.asDateTime = asDateTime
    w_wset_data = w.wset("corporationaction", param)
    df = pd.DataFrame(w_wset_data.Data)
    df = df.T
    df.columns = w_wset_data.Fields

    df['ex_dividend_date'] = pd.to_datetime(df['ex_dividend_date'])

    return df


def download_seoimplementation(w, startdate, enddate):
    """
    增发实施
    :param w:
    :param startdate:
    :param enddate:
    :return:
    """
    w.asDateTime = asDateTime
    w_wset_data = w.wset("seoimplementation",
                         "startdate=%s;enddate=%s;sectorid=a001010100000000;windcode=" % (startdate, enddate))
    df = pd.DataFrame(w_wset_data.Data)
    df = df.T
    df.columns = w_wset_data.Fields

    df['seo_anncedate'] = pd.to_datetime(df['seo_anncedate'])
    df['issue_date'] = pd.to_datetime(df['issue_date'])
    df['unlocking_time'] = pd.to_datetime(df['unlocking_time'])

    return df
