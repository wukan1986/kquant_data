#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
调用wset函数的部分

单位累计分红可以间接拿到分红日期
w.wsd("510050.SH", "div_accumulatedperunit", "2016-01-25", "2017-02-23", "")

"""
from ..processing.utils import *
from .utils import asDateTime


def download_daily_at(
        w,
        codes,
        fields,
        date,
        option="Fill=Previous;PriceAdj=F"):
    """
    下载具体某一天的数据，这种数据一般不是很多
    :param codes:
    :param fields:
    :param beginTime:
    :param endTime:
    :param columns:
    :return:
    """
    w.asDateTime = asDateTime
    w_wsd_data = w.wsd(codes, fields, date, date, option)
    df = pd.DataFrame(w_wsd_data.Data, )
    # 主要得看数据是多行还是一行，只有一行，就不用转置了
    # df = df.T
    df.columns = w_wsd_data.Codes
    df.index = w_wsd_data.Times
    return df


def download_daily_between(
        w,
        codes,
        field,
        beginTime,
        endTime,
        option='unit=1;rptType=1;Period=Q;Days=Alldays'):
    """
    下载年报，季报，中报
    下载是多个合约的一个字段
    这种方法适合于一次性的准备数据
    Q/M/W
    :param w:
    :param codes:
    :param fields:
    :param beginTime:
    :param endTime:
    :return:
    """
    w.asDateTime = asDateTime
    w_wsd_data = w.wsd(codes, field, beginTime, endTime, option)
    df = pd.DataFrame(w_wsd_data.Data, )
    df = df.T  # 多行数据，得转置
    df.columns = w_wsd_data.Codes
    df.index = w_wsd_data.Times
    return df


def download_daily_from_beginning(w, ipo_date, enddate, wind_code, field):
    """
    下载第一天的数据，和以后每季的数据，最新一天会下载过来
    :param w:
    :param ipo_date:
    :param enddate:
    :param wind_code:
    :param field:
    :return:
    """
    option = "unit=1;rptType=1"
    # 因为这天是ipo，应当不会
    df_1 = download_daily_at(w, wind_code, field, ipo_date, option)
    option = "unit=1;rptType=1;Period=Q"
    df_2 = download_daily_between(w, wind_code, field, ipo_date, enddate, option)

    df = pd.concat([df_1, df_2])
    # 这里可能出现重复数据，需要清理一下
    df = df[~df.index.duplicated(keep='last')]

    return df


def download_daily_between_for_series(
        w,
        series,
        code,
        field,
        trading_days):
    """
    只是在已经有两个端点数据的情况下的补充数据
    :param w:
    :param series:
    :param code:
    :param field:
    :param trading_days:
    :return:
    """
    # 有可能过来的数据索引可能有重复，要处理一下
    series = series[~series.index.duplicated(keep='first')]
    # 数据去重有问题，可能出现AAABAAAC这类的情况，如600052.SH中间有一段数据，这样判断会出错
    # series_bool = ~series.duplicated(keep='last')
    # 下面是换一种算法来处理
    rolling_count.previous = None
    _count_ = series.apply(rolling_count)
    # 这记录的是开头
    _first_ = _count_ == 0
    _last_ = _first_.shift(-1)
    _last_[-1] = True
    series_bool = _last_

    series_data = series
    run = False
    set_data = set(series_data)
    # 这只能处理至少两个点的情况
    for i in range(0, len(series_data) - 1):
        if series_bool.iat[i]:
            date_start = series_bool.index[i]
            date_end = series_bool.index[i + 1]
            # 由于从wind下载的数据最后有毫秒0500，取时间会导致跳过第一段了
            part_days = trading_days[date_start.strftime('%Y-%m-%d'):date_end.strftime('%Y-%m-%d')]
            # 如果两个时间已经贴近了就没有必要再下载了
            if len(part_days) <= 2:
                continue
            # 以季或周下载时，开头的数据是按季或周来，最后一个数据是指定日
            days = (date_end - date_start).days
            days = len(part_days)
            if days > 70:
                # 日子不能设得太短，不然一直是下的季报，结果是重复的，91和89都出现过
                option = "unit=1;rptType=1;Period=Q"
            elif days < 15:
                option = "unit=1;rptType=1"
            else:
                option = "unit=1;rptType=1;Period=W"
            df_new = download_daily_between(w, code, field,
                                            # 前开后闭，因为有可能最后一天不一样
                                            part_days.index[1], part_days.index[-1],
                                            option)
            print('补数据:%s,%s,%s' % (part_days.index[1], part_days.index[-1], option))
            # 也许要判断一下,不知道会不会有空数据出现
            s = df_new.iloc[:, 0]
            print(s)

            series_data = series_data.combine_first(s)
            # 标记一下进行过修改，还可以再测下一轮，否则不用
            # 如果正好在换月的那天，A/B/nan出现时，会导致死循环下载
            diff_data = set(series_data) - set_data
            run = len(diff_data)>0

    if run:
        # 如果一轮都没有下载数据，就不能再进入到循环中了
        series_data = download_daily_between_for_series(w, series_data, code, field, trading_days)
    # 下载的数据两条索引是重复的，需要去重
    series_data = series_data[~series_data.index.duplicated(keep='last')]

    return series_data
