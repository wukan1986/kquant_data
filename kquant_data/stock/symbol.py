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
import os
import pandas as pd


def is_stock(symbol):
    _symbol = symbol.lower()
    if _symbol.startswith('sz00') or _symbol.startswith('sz30') or _symbol.startswith('sh60'):
        return True
    return False


def is_index(symbol):
    _symbol = symbol.lower()
    if _symbol.startswith('sh000001'):
        return True
    return False


def is_gc_rc(symbol):
    _symbol = symbol.lower()
    if _symbol.startswith('sh204') or _symbol.startswith('sz131'):
        return True
    return False


def filter_symbol(symbol):
    # 将6,0,3以外的都过滤掉
    if is_stock(symbol) or is_index(symbol) or is_gc_rc(symbol):
        return True
    return False


def get_symbols_from_path_only_stock(path, exchange):
    """
    指定目录，将目录转成合约列表
    :param path:
    :param exchange:
    :return:
    """
    list1 = []
    list2 = []
    list3 = []
    list4 = []
    list5 = []
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            if is_stock(filename):
                list1.append(filename[:8])
                list2.append(filename[:2])
                list3.append(filename[2:8])
                list4.append("%s.%s" % (filename[2:8], exchange))
                list5.append("%s.%s" % (filename[2:8], filename[:2].upper()))

    df = pd.DataFrame({"local_symbol": list1, "market": list2, "code": list3, "symbol": list4, "wind_code": list5})

    return df


def get_symbols_from_path(path, exchange):
    """
    指定目录，将目录转成合约列表
    :param path:
    :param exchange:
    :return:
    """
    list1 = []
    list2 = []
    list3 = []
    list4 = []
    list5 = []
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            list1.append(filename[:8])
            list2.append(filename[:2])
            list3.append(filename[2:8])
            list4.append("%s.%s" % (filename[2:8], exchange))
            list5.append("%s.%s" % (filename[2:8], filename[:2].upper()))

    df = pd.DataFrame({"local_symbol": list1, "market": list2, "code": list3, "symbol": list4, "wind_code": list5})

    return df


def get_symbols_from_wind_code(wind_codes):
    """

    :param wind_codes:
    :return:
    """
    list1 = []
    list2 = []
    list3 = []
    list4 = []
    list5 = []
    for wind_code in wind_codes:
        # print(wind_code)
        list1.append("%s%s" % (wind_code[-2:].lower(), wind_code[:6]))
        list2.append(wind_code[-2:].lower())
        list3.append(wind_code[:6])
        list4.append("%s.%s" % (wind_code[:6], wind_code[-2:].upper()))
        list5.append(wind_code)

    df = pd.DataFrame({"local_symbol": list1, "market": list2, "code": list3, "symbol": list4, "wind_code": list5})

    return df


def get_folder_symbols(folder, sub_folder):
    path = os.path.join(folder, sub_folder, 'sh')
    df_sh = get_symbols_from_path(path, "SSE")
    path = os.path.join(folder, sub_folder, 'sz')
    df_sz = get_symbols_from_path(path, "SZSE")
    df = pd.concat([df_sh, df_sz])

    return df
