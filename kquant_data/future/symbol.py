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

CFE = ['IF', 'IC', 'IH', 'T', 'TF', 'AF', 'EF', 'TS', 'TT']
SHF = ['cu', 'al', 'zn', 'pb', 'ni', 'sn', 'au', 'ag', 'rb', 'wr', 'hc', 'fu', 'bu', 'ru'] # , 'im'这个只是指数
CZC = ['SR', 'CF', 'ZC', 'FG', 'TA', 'WH', 'PM', 'RI', 'LR', 'JR', 'RS', 'OI', 'RM', 'SF', 'SM', 'MA', 'WT', 'WS', 'RO',
       'ER', 'ME', 'TC', 'CY']
DCE = ['m', 'y', 'a', 'b', 'p', 'c', 'cs', 'jd', 'fb', 'bb', 'l', 'v', 'pp', 'j', 'jm', 'i']

# 不活跃的合约
InactiveProducts = ['b', 'fu', 'wr', 'JR', 'bb', 'LR', 'fb', 'RI', 'PM', 'RS', 'SF', 'SM']
# 改名前的合约或过期的合约
ExpiredProducts = ['RO', 'ER', 'WS', 'ME', 'WT', 'TC', 'AF', 'EF', 'TS', 'TT']  # 从AF开始的几个是没有上市的
# 有夜盘的
NightProducts = ['RO', 'ER', 'WS', 'ME', 'WT', 'TC']


def is_inactvie_product(product):
    return product in InactiveProducts


def get_all_products():
    return list(set(CFE + SHF + CZC + DCE) - set(ExpiredProducts))


def get_actvie_products():
    return list(set(CFE + SHF + CZC + DCE) - set(ExpiredProducts) - set(InactiveProducts))


def get_wind_code(product):
    if product in CFE:
        return product + '.CFE'
    if product in SHF:
        return product + '.SHF'
    if product in CZC:
        return product + '.CZC'
    if product in DCE:
        return product + '.DCE'

def get_wind_code_S(product):
    if product in CFE:
        return product + '_S.CFE'
    if product in SHF:
        return product + '_S.SHF'
    if product in CZC:
        return product + '_S.CZC'
    if product in DCE:
        return product + '_S.DCE'


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


if __name__ == '__main__':
    x = get_actvie_products_wind()
    print(x)
    print(1)
