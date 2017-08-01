#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
合约名处理方法
由于数据来源不同，导致合约名需要另行定义

市场名和交易所名是不一样的
股票和期货要别行处理
SSE     SH
SZSE    SZ
SHFE    SHF
DCE     DCE
CZCE    CZC
CFFEX   CFE
"""
import re


def exchange_2_wind_market(exchange):
    """
    将交易所缩写转换成万得交易所代码
    :param exchange:
    :return:
    """
    ret = {
        'SSE': 'SH',
        'SZSE': 'SZ',
        'SHFE': 'SH',
        'DCE': 'DCE',
        'CZCE': 'CZC',
        'CFFEX': 'CFE',
    }[exchange.upper()]
    return ret


def split_by_dot(symbol):
    """
    只是根据中间出现的点来分隔，需要由其它方法来灵活调用
    print(split_by_dot("00000.SH"))
    print(split_by_dot("SH60000"))

    ['00000', 'SH']
    ['SH60000']

    :param symbol:
    :return:
    """
    return symbol.split('.')


def split_alpha_number(symbol):
    """
    分离字母开头，然后接数字的情况，如果后面有别的字符会被截断
    print(split_alpha_number("TF1703.CFE"))
    ('TF', '1703')
    :param symbol:
    :return:
    """
    pattern = re.compile(r'([A-Za-z]+)(\d+)')
    match = pattern.match(symbol)
    if match:
        return match.group(1, 2)
    return None