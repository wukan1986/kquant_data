#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票处理方法
"""
import multiprocessing
import os
from functools import partial

import pandas as pd

from ..processing.utils import filter_dataframe
from ..utils.xdatetime import datetime_2_yyyyMMdd____, yyyyMMddHHmm_2_datetime, tic, toc, yyyyMMdd_2_datetime
from .dzh import DzhDividend, dividend_to_h5
from .tdx import read_file, bars_to_h5, get_h5_86400_path, get_tdx_path


def sort_dividend(divs):
    """
    对权息信息按时间排序
    :param divs:
    :return:
    """
    if len(divs) > 0:
        df = pd.DataFrame(divs)
        df = df.sort_values(by='time')

        df.time = df.time.apply(lambda x: pd.datetime.utcfromtimestamp(x))
        df = df.set_index('time')

    return df


def factor(daily, divs):
    """
    计算得到向后复权因子
    发现这种算法没有错，但很多股票还是有一些与万得对应不上
    除权应子的算法应当是 交易所会发布前收盘价与收盘价进行比较就是除权因子
    但上交所网站上前收盘价并不好查，因为按分类，有些还是不好做
    可以对照一下通达信与万得的行情，哪种价格对应得上
    :param daily:
    :param divs:
    :return:
    """
    # 排序复权因子,一定要用，因为会更新时间格式
    if False:
        df = sort_dividend(divs)
    else:
        df = divs
        df.loc[:, 'time'] = df.loc[:, 'time'].apply(lambda x: yyyyMMdd_2_datetime(x))
        df = df.set_index('time')

    # 过滤一下，用来计算除权价
    daily_part = daily[['DateTime', 'DateTime', 'Close']]
    daily_part.columns = ['time', 'pre_day', 'pre_close']
    first_day = daily_part.index[0]
    last_day = daily_part.index[-1]

    # 无语，停牌会选不出来，比如说SZ000001，会有日期对应不上,所以只能先合并然后再处理
    daily_div = pd.merge(daily_part, df, how='outer', left_index=True, right_index=True, sort=False)
    # 由于可能出现在停牌期公布除权除息，所以需要补上除权那天的收盘价
    daily_div['pre_close'] = daily_div['pre_close'].fillna(method='pad', limit=1)
    daily_div = daily_div.fillna(method='pad', limit=1)
    daily_div[['time', 'pre_day', 'pre_close']] = daily_div[['time', 'pre_day', 'pre_close']].shift(1)
    daily_div[['split', 'purchase', 'purchase_price', 'dividend']] = daily_div[
        ['split', 'purchase', 'purchase_price', 'dividend']].fillna(method='bfill', limit=1)

    # 预处理后只取需要的部分
    df = daily_div.loc[df.index]
    # 发现部分股票会提前公布除权除息信息，导致后面比例出错

    df = df.fillna(0)

    # 除权价
    df['dr_pre_close'] = (df['pre_close'] - df['dividend'] + df['purchase'] * df['purchase_price']) / (
            1 + df['split'] + df['purchase'])
    # 要做一次四舍五入,不然除权因子不对,2是不是不够，需要用到3呢？
    df['dr_pre_close'] = df['dr_pre_close'].apply(lambda x: round(x, 2))
    # 除权因子
    df['dr_factor'] = df['pre_close'] / df['dr_pre_close']

    # 将超出日线还没有实现或没有行情的除权因子改成1，注意可能因为通达信没有完全下载数据而导致出错
    df.loc[df.index > last_day, 'dr_factor'] = 1
    # 这个地方会有风险，能拿到更全的数据最好
    df.loc[df.index < first_day, 'dr_factor'] = 1

    # 在最前插件一条特殊的记录，用于表示在第一次除权之前系数为1
    # 由于不知道上市是哪一天，只好用最小日期
    first_ = pd.DataFrame({'dr_factor': 1}, index=[pd.datetime(1900, 1, 1)])
    df = df.append(first_)
    df = df.sort_index()

    df['time'] = df.index.map(datetime_2_yyyyMMdd____)

    # 向后复权因子，注意对除权因子的累乘
    df['backward_factor'] = df['dr_factor'].cumprod()
    # 向前复权因子
    df['forward_factor'] = df['backward_factor'] / float(df['backward_factor'][-1:])

    df = df[['time', 'pre_day', 'pre_close',
             'split', 'purchase', 'purchase_price', 'dividend',
             'dr_pre_close', 'dr_factor', 'backward_factor', 'forward_factor']]

    return df


def adjust(df, adjust_type=None):
    if adjust_type is None:
        return df
    adjust_type = adjust_type.lower()

    if adjust_type.startswith('f'):
        df['Open'] = df['Open'] * df['forward_factor']
        df['High'] = df['High'] * df['forward_factor']
        df['Low'] = df['Low'] * df['forward_factor']
        df['Close'] = df['Close'] * df['forward_factor']
        return df

    if adjust_type.startswith('b'):
        df['Open'] = df['Open'] * df['backward_factor']
        df['High'] = df['High'] * df['backward_factor']
        df['Low'] = df['Low'] * df['backward_factor']
        df['Close'] = df['Close'] * df['backward_factor']
        return df

    return df


def merge_adjust_factor(df, div):
    """
    从通达信中读出的原始数据，与从除权表中读出的原始数据合并处理成一个表，可以将此表另行保存备用
    :param df:
    :param div:
    :return:
    """
    div['index_datetime'] = div['time'].apply(yyyyMMddHHmm_2_datetime)
    div = div.set_index('index_datetime')

    div = div[['backward_factor', 'forward_factor']]
    div.columns = ['backward_factor_tmp', 'forward_factor_tmp']

    df_div = pd.merge(df, div, left_index=True, right_index=True, how='outer')
    df_div[['backward_factor', 'forward_factor']] = df_div[['backward_factor_tmp', 'forward_factor_tmp']].fillna(
        method='ffill')
    del df_div['backward_factor_tmp']
    del df_div['forward_factor_tmp']
    df_div = df_div.loc[df.index]
    return df_div


def read_h5_tdx(market, code, bar_size, h5_path, tdx_path, div_path):
    """
    指定时间，市场，代码，返回数据
    :param market:
    :param code:
    :param bar_size:
    :return:
    """
    if bar_size == 86400:
        # 日线优先从h5格式中取
        _h5_path = os.path.join(h5_path, get_h5_86400_path(market, code, bar_size))
        try:
            df = pd.read_hdf(_h5_path)
            # print(df.dtypes)
            df = filter_dataframe(df, 'DateTime', None, None, None)
            return df
        except Exception as e:
            # 数据没有取出来
            print(e)
            pass
    else:
        # 其它数据就直接取原始数据，这样省事
        pass

    _tdx_path = os.path.join(tdx_path, get_tdx_path(market, code, bar_size))
    try:
        df = read_file(_tdx_path)
    except FileNotFoundError:
        # 没有原始的数据文件
        return None

    # 有可能没有除权文件
    _div_path = os.path.join(div_path, "%s%s.h5" % (market, code))
    try:
        div = pd.read_hdf(_div_path)
        df = merge_adjust_factor(df, div)
    except:
        # 这里一般是文件没找到，表示没有除权信息
        df['backward_factor'] = 1
        df['forward_factor'] = 1

    return df


def _export_dividend_from_data(tdx_root, dividend_output, daily_output, data):
    """
    除权信息的导出，并导出日线
    :param tdx_input:
    :param dzh_output:
    :param daily_output:
    :param data:
    :return:
    """
    symbol = data[0]
    divs = data[1]
    print(symbol)

    if False:
        _symbol = symbol.lower().decode('utf-8')
    else:
        _symbol = symbol
        divs = divs[
            ['datetime', 'songgu_qianzongguben', 'peigu_houzongguben', 'peigujia_qianzongguben',
             'hongli_panqianliutong']]
        divs.columns = ['time', 'split', 'purchase', 'purchase_price', 'dividend']
        # 通达信中记录的是每10股，大智慧中记录的是每1股，这里转成大智慧的格式
        divs[['split', 'purchase', 'dividend']] /= 10

    dividend_output_path = os.path.join(dividend_output, _symbol + '.h5')

    if _symbol.startswith('sh'):
        daily_input_path = os.path.join(tdx_root, 'sh', 'lday', _symbol + '.day')
        daily_output_path = os.path.join(daily_output, 'sh', _symbol + '.h5')
    else:
        daily_input_path = os.path.join(tdx_root, 'sz', 'lday', _symbol + '.day')
        daily_output_path = os.path.join(daily_output, 'sz', _symbol + '.h5')

    try:
        # 宏证证券000562退市了，导致找不到股票行情，但还有除权数据
        tdx_daily = read_file(daily_input_path)
        df_divs = factor(tdx_daily, divs)

        # 保存时还是把权息给加上，这样少了调用时的合并操作
        daily_divs = merge_adjust_factor(tdx_daily, df_divs)
        del df_divs['index_datetime']

        # 保存
        bars_to_h5(daily_output_path, daily_divs)
        dividend_to_h5(dividend_output_path, df_divs)
    except Exception as e:
        print(e)


def export_dividend_daily_dzh(dzh_input, tdx_root, dividend_output, daily_output):
    """
    导出除权数据，并同时生成对应的日线数据
    :param tdx_input:
    :param dzh_input:
    :param dzh_output:
    :param daily_output:
    :return:
    """
    io = DzhDividend(dzh_input)
    r = io.read()

    tic()

    multi = False
    if multi:
        # 多进程并行计算
        pool_size = multiprocessing.cpu_count() - 1
        pool = multiprocessing.Pool(processes=pool_size)
        func = partial(_export_dividend_from_data, tdx_root, dividend_output, daily_output)
        pool_outputs = pool.map(func, list(r))
        print('Pool:', pool_outputs)
    else:
        # 单线程
        for d in list(r):
            _export_dividend_from_data(tdx_root, dividend_output, daily_output, d)

    toc()


def export_dividend_daily_gbbq(gbbq_input, tdx_root, dividend_output, daily_output):
    """
    导出除权数据，并同时生成对应的日线数据
    :param tdx_input:
    :param dzh_input:
    :param dzh_output:
    :param daily_output:
    :return:
    """
    df = pd.read_csv(gbbq_input, index_col=0, dtype={'code': str})
    # 只取除权信息
    df = df[df['category'] == 1]
    df['exchange'] = df['market'].replace(0, "sz").replace(1, 'sh')
    df['symbol'] = df['exchange'] + df['code']
    div_list = [(name, group) for name, group in df.groupby(by=['symbol'])]

    tic()

    multi = True
    if multi:
        # 多进程并行计算
        pool_size = multiprocessing.cpu_count()
        if pool_size > 2:
            pool_size -= 1
        pool = multiprocessing.Pool(processes=pool_size)
        func = partial(_export_dividend_from_data, tdx_root, dividend_output, daily_output)
        pool_outputs = pool.map(func, div_list)
        print('Pool:', pool_outputs)
    else:
        # 单线程
        for d in div_list:
            _export_dividend_from_data(tdx_root, dividend_output, daily_output, d)

    toc()
