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


def download_indexconstituent(w, date, windcode, field='wind_code,i_weight'):
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


def download_optioncontractbasicinfo(w, exchange='sse', windcode='510050.SH', status='trading',
                                     field='wind_code,trade_code,sec_name,contract_unit,listed_date,expire_date,reference_price'):
    """
    指数权重
    如果指定日期不是交易日，会返回时前一个交易日的信息
    :param w:
    :param windcode:
    :param date:
    :return:
    """
    param = 'exchange=%s' % exchange
    param += ';windcode=%s' % windcode
    param += ';status=%s' % status
    if field:
        param += ';field=%s' % field

    w.asDateTime = asDateTime
    w_wset_data = w.wset("optioncontractbasicinfo", param)
    df = pd.DataFrame(w_wset_data.Data)
    df = df.T
    df.columns = w_wset_data.Fields
    return df


def download_optionchain(w, date='2017-11-28', us_code='510050.SH',
                         field='option_code,option_name,strike_price,multiplier'):
    """
    下载指定日期期权数据

w_wset_data = vba_wset("optionchain","date=2017-11-28;us_code=510050.SH;option_var=全部;call_put=全部;field=option_code,option_name,strike_price,multiplier",)
    :param w:
    :param windcode:
    :param date:
    :return:
    """
    param = 'date=%s' % date
    param += ';us_code=%s' % us_code
    if field:
        param += ';field=%s' % field

    w.asDateTime = asDateTime
    w_wset_data = w.wset("optionchain", param)
    df = pd.DataFrame(w_wset_data.Data)
    df = df.T
    df.columns = w_wset_data.Fields
    return df


def read_constituent(path):
    """
    读取板块文件
    :param path:
    :return:
    """
    try:
        df = pd.read_csv(path, encoding='utf-8-sig', parse_dates=True)
    except Exception as e:
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
            curr_df = read_constituent(filepath)
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


def write_constituent(path, df):
    df.to_csv(path, encoding='utf-8-sig', date_format='%Y-%m-%d', index=False)


def read_indexconstituent_from_dir(path):
    """
    由于权重每天都不一样，只能根据用户指定的日期下载才行
    :param path:
    :return:
    """
    last_set = None
    df = None
    for parent, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(parent, filename)
            curr_df = read_constituent(filepath)
            # 2016-12-12,有成份新加入，但权重为nan
            curr_df.fillna(0, inplace=True)

            data_date_str = filename[:-4]
            curr_df['_datetime_'] = pd.to_datetime(data_date_str)
            if df is None:
                df = curr_df
            else:
                df = pd.concat([df, curr_df])
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


def download_futureoir(w, startdate, enddate, windcode):
    """
    品种持仓
    是将多仓与空仓排名合并到一起，然后按多仓进行统一排名，所以对于空仓要使用时，需要自行重新排名
    查询的跨度不要太长，一个月或三个月
    :param w:
    :param startdate:
    :param enddate:
    :return:
    """
    w.asDateTime = asDateTime
    w_wset_data = w.wset("futureoir",
                         "startdate=%s;enddate=%s;varity=%s;order_by=long;ranks=all;"
                         "field=date,ranks,member_name,"
                         "long_position,long_position_increase,long_potion_rate,"
                         "short_position,short_position_increase,short_position_rate,"
                         "vol,vol_increase,vol_rate,settle" % (startdate, enddate, windcode))
    df = pd.DataFrame(w_wset_data.Data)
    df = df.T
    df.columns = w_wset_data.Fields

    try:
        df['date'] = pd.to_datetime(df['date'])
    except:
        pass

    return df
