#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
调用wset函数的部分

单位累计分红可以间接拿到分红日期
w.wsd("510050.SH", "div_accumulatedperunit", "2016-01-25", "2017-02-23", "")

"""
import os
from datetime import datetime

import numpy as np
import pandas as pd

from ..processing.utils import series_drop_duplicated_keep_both_rolling
from ..wind.wsd import download_daily_at, download_daily_between, download_daily_from_beginning, \
    download_daily_between_for_series
from ..xio.csv import write_data_dataframe, read_data_dataframe, read_datetime_dataframe, write_datetime_dataframe


def resume_download_daily_many_to_one_file(
        w,
        wind_codes,
        field,
        dtype,
        enddate,
        root_path,
        option='unit=1;rptType=1;Period=Q;Days=Alldays'):
    """
    增量下载，下载每个季度因子到一个文件，由于季度使用日历日
    按日下数据也没有关系，一般是每天都不一样才使用，这时使用交易日

    # 对于有些因子，可能公司有问题，很久不公布，可能过了一年都不公布
    # 这种一般是退市或停牌了，对于这些，找一个时间更新一下即可
    # 补最近的两个报告
    dr = pd.date_range(end=datetime.datetime.today().date(), periods=4, freq='Q')
    new_end_str = dr[-1].strftime('%Y-%m-%d')
    :param w:
    :param wind_codes:
    :param field:
    :param enddate:
    :param option:
    :param root_path:
    :return:
    """
    df_old = None
    df_old_output = None
    new_end_str = enddate

    path = os.path.join(root_path, '%s.csv' % field)
    # 补数据时，将半年内的数据再重下一次
    try:
        df_old = pd.read_csv(path, index_col=0, parse_dates=True, encoding='utf-8-sig')
        df_old_output = df_old.copy()

        # 如果最后日子不相等，表示是以前下的，需要多考虑一天
        old_end_str = df_old.index[-1].strftime('%Y-%m-%d')
        back = -2
        if old_end_str != new_end_str:
            back = -3
        new_start_str = df_old.index[back].strftime('%Y-%m-%d')
        df_old = df_old[:back]
    except:
        # 打开失败，使用老数据
        new_start_str = '2000-01-01'

    df_new = download_daily_between(w, wind_codes, field,
                                    new_start_str, new_end_str, option)

    if df_old is None:
        df = df_new
    else:
        # 合并，第一，需要两者列相同
        df = pd.DataFrame(index=df_old.index, columns=df_new.columns)
        # 这里有可能重复
        df[df_old.columns] = df_old
        df = pd.concat([df, df_new])

    # 数据内容实际是时间，需要强行转换，这样后面写csv时就不会出现后面一段了
    # 排序会不会出问题呢？
    if dtype == np.datetime64:
        df = df.stack(dropna=False)
        df = pd.to_datetime(df)
        df = df.unstack()

    df = df[wind_codes]
    write_data_dataframe(path, df)
    print(path)
    # 输出这前后两个DataFrame可以提供给外界判断是否有新数据要下载
    return df_old_output, df


def resume_download_daily_one_to_one_file(
        w,
        symbol,
        ipo_date,
        field,
        trading_days,
        root_path):
    """
    按合约使用特殊算法快速下载数据
    比如total_shares
    对于已经下载完成的数据，以后再说了
    :param w:
    :param symbol:
    :param ipo_date:
    :param field:
    :param trading_days:
    :param root_path:
    :return:
    """
    path = os.path.join(root_path, field, '%s.csv' % symbol)

    df_old = read_data_dataframe(path)
    if df_old is None:
        # 看是否需要下最新一段？不下了，要下使用新的下法，一次下一行
        date_str = datetime.today().strftime('%Y-%m-%d')
        df_old = download_daily_from_beginning(w, ipo_date, date_str, symbol, field)

    series = download_daily_between_for_series(w, df_old.iloc[:, 0], symbol, field, trading_days)
    # 对数据进行清理
    series = series_drop_duplicated_keep_both_rolling(series)

    return series


def resume_download_daily_to_many_files(
        w,
        trading_days,
        ipo_date,
        root_path,
        field='total_shares'):
    """
    全部下载数据
    :param w:
    :param trading_days:
    :param ipo_date:
    :param field:
    :param root_path:
    :return:
    """
    for i in range(0, ipo_date.shape[1]):
        symbol = ipo_date.iloc[:, i]
        # if symbol.name != '601200.SH':
        #     continue
        print(symbol)
        # 600849没有ipo日期
        if symbol[0] is pd.NaT:
            print('没有IPO日期')
            continue

        date_str = symbol[0].strftime('%Y-%m-%d')

        ss = resume_download_daily_one_to_one_file(w, symbol.name, date_str, field, trading_days, root_path)

        # 保存
        path = os.path.join(root_path, field, '%s.csv' % symbol.name)
        df = pd.DataFrame(ss)
        write_data_dataframe(path, df)


def resume_download_delist_date(
        w,
        wind_codes,
        root_path,
        field='delist_date',
        dtype=np.datetime64):
    """
    下载每支股票的delist_date
    如果以后有同类的每个股票一个数，但可能上新股票都得更新的field就可以用
    :param w:
    :param wind_codes:
    :param field:
    :param dtype:
    :param root_path:
    :return:
    """
    wind_codes_set = set(wind_codes)

    date_str = datetime.today().strftime('%Y-%m-%d')

    path = os.path.join(root_path, '%s.csv' % field)
    if dtype == np.datetime64:
        df_old = read_datetime_dataframe(path)
    else:
        df_old = read_data_dataframe(path)

    if df_old is None:
        new_symbols = wind_codes_set
    else:
        df_old.dropna(axis=1, inplace=True)
        new_symbols = wind_codes_set - set(df_old.columns)

    # 没有新数据好办，只有一个数据怎么办？会出错吗
    if len(new_symbols) == 0:
        print('没有空合约，没有必要更新%s' % field)
        # 可能排序不行，还是再处理下
        df_new = df_old.copy()
    else:
        # 第一次下全，以后每次下最新的
        df_new = download_daily_at(w, list(new_symbols), field, date_str)

    # 新旧数据的合并
    df = pd.DataFrame(columns=wind_codes)
    if df_old is not None:
        df[df_old.columns] = df_old
        df.index = df_new.index
        df[df_new.columns] = df_new
    else:
        df = df_new

    # 排序有点乱，得处理
    df = df[wind_codes]
    if dtype == np.datetime64:
        write_datetime_dataframe(path, df)
    else:
        write_data_dataframe(path, df)


def resume_download_financial_report_data_daily_latest(
        w,
        wind_codes,
        trading_days,
        root_path,
        field='total_shares'):
    """
    下载全市场最新一天的数据，然后与历史的合并
    合并后的数据中间有部分需要补充，交给另一个过程来补
    :param w:
    :param trading_days:
    :param field:
    :return:
    """
    # 下载最新一天的数据
    date_str = datetime.today().strftime('%Y-%m-%d')
    df_new = download_daily_at(w, wind_codes, field, date_str, "")
    path = os.path.join(root_path, 'temp', '%s.csv' % field)
    write_data_dataframe(path, df_new)

    # 把数据合并过去，合并的同时下载数据，还是只合并？还是只合并吧，这样分步来也快
    for i in range(0, df_new.shape[1]):
        s = df_new.iloc[:, i]
        print(s)
        path = os.path.join(root_path, field, '%s.csv' % s.name)
        df_old = read_data_dataframe(path)
        if df_old is None:
            # 如果还没有历史数据就跳过，让系统通过ipo_date文件再自动补充
            continue
        # df_old = df_old[:-1]
        ss = df_old.iloc[:, 0]
        series = ss.combine_first(s)

        # 对数据进行清理
        series = series_drop_duplicated_keep_both_rolling(series)
        series = pd.DataFrame(series)
        # 保存
        write_data_dataframe(path, series)


def resume_download_financial_report_quarter(w, wind_codes, root_path, fields=['stm_issuingdate'], dtype=np.datetime64):
    """

    其实发现有更新的部分，然后再增量下载其它字段是最合适不过的了，目前此功能还没有实现
    :param w:
    :param wind_codes:
    :return:
    """
    dr = pd.date_range(end=datetime.today().date(), periods=4, freq='Q')
    new_end_str = dr[-1].strftime('%Y-%m-%d')

    for field in fields:
        resume_download_daily_many_to_one_file(w,
                                               wind_codes,
                                               field,
                                               dtype, new_end_str,
                                               root_path,
                                               option='unit=1;rptType=1;Period=Q;Days=Alldays')
