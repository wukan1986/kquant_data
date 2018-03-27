#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
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
'''
import os
import pandas as pd
import re

CZCE_pattern = re.compile(r'(\D{1,2})(\d{0,1})(\d{3})(.*)')

CFE = ['IF', 'IC', 'IH', 'T', 'TF', 'AF', 'EF', 'TS', 'TT']
SHF = ['cu', 'al', 'zn', 'pb', 'ni', 'sn', 'au', 'ag', 'rb', 'wr', 'hc', 'fu', 'bu', 'ru']  # , 'im'这个只是指数
CZC = ['SR', 'CF', 'ZC', 'FG', 'TA', 'WH', 'PM', 'RI', 'LR', 'JR', 'RS', 'OI', 'RM', 'SF', 'SM', 'MA', 'WT', 'WS', 'RO',
       'ER', 'ME', 'TC', 'CY', 'AP']
DCE = ['m', 'y', 'a', 'b', 'p', 'c', 'cs', 'jd', 'fb', 'bb', 'l', 'v', 'pp', 'j', 'jm', 'i']
INE = ['sc']

# 不活跃的合约
InactiveProducts = ['b', 'fu', 'wr', 'JR', 'bb', 'LR', 'fb', 'RI', 'PM', 'RS', 'SF', 'SM']
# 改名前的合约或过期的合约
ExpiredProducts = ['RO', 'ER', 'WS', 'ME', 'WT', 'TC', 'AF', 'EF', 'TS', 'TT', 'im']  # 从AF开始的几个是没有上市的
# 有夜盘的
NightProducts = ['RO', 'ER', 'WS', 'ME', 'WT', 'TC']


def is_inactvie_product(product):
    return product in InactiveProducts


def get_all_products():
    return list(set(CFE + SHF + CZC + DCE + INE) - set(ExpiredProducts))


def get_actvie_products():
    return list(set(CFE + SHF + CZC + DCE + INE) - set(ExpiredProducts) - set(InactiveProducts))


def get_wind_code(product):
    if product in CFE:
        return product + '.CFE'
    if product in SHF:
        return product + '.SHF'
    if product in CZC:
        return product + '.CZC'
    if product in DCE:
        return product + '.DCE'
    if product in INE:
        return product + '.INE'


def get_wind_code_S(product):
    if product in CFE:
        return product + '_S.CFE'
    if product in SHF:
        return product + '_S.SHF'
    if product in CZC:
        return product + '_S.CZC'
    if product in DCE:
        return product + '_S.DCE'
    if product in INE:
        return product + '_S.INE'


def get_all_products_wind():
    _lst = get_all_products()
    __lst = [get_wind_code(x).upper() for x in _lst]
    return __lst


def get_all_products_wind_S():
    _lst = get_all_products()
    __lst = [get_wind_code_S(x).upper() for x in _lst]
    return __lst


def get_actvie_products_wind():
    _lst = get_actvie_products()
    __lst = [get_wind_code(x).upper() for x in _lst]
    return __lst


def CZCE_3to4(symbol3, y3=1):
    match = CZCE_pattern.match(symbol3)
    # 有就直接用，没有就得填，到2000年怎么办，到2020年怎么办
    if not match:
        return symbol3
    num1 = match.group(2)
    if len(num1) > 0:
        return symbol3

    return "%s%d%s%s" % (match.group(1), y3, match.group(3), match.group(4))


def CZCE_4to3(symbol4):
    match = CZCE_pattern.match(symbol4)
    # 有就直接用，没有就得填，到2000年怎么办，到2020年怎么办
    if not match:
        return symbol4

    return "%s%s%s" % (match.group(1), match.group(3), match.group(4))


def CZCE_convert(symbol):
    match = CZCE_pattern.match(symbol)
    # 有就直接用，没有就得填，到2000年怎么办，到2020年怎么办
    if not match:
        return symbol

    num1 = match.group(2)
    num3 = match.group(3)
    # 这里老要改
    if num3 > '606' and num1 == '1':
        return "%s%s%s" % (match.group(1), match.group(3), match.group(4))
    if len(num1) == 0 and num3 <= '606':
        return "%s1%s%s" % (match.group(1), match.group(3), match.group(4))
    return symbol


if __name__ == '__main__':
    x = get_actvie_products_wind()
    print(x)
    print(CZCE_3to4('RB807'))
    print(CZCE_3to4('RB807.CZC'))
