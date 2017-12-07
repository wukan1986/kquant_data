#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
主要处理与时间有关的功能
如果已经有公共库实现了相关的功能，可以将这里对应的地方注释，然后加上公共库的示例代码放在这里
"""
import time
from datetime import datetime


def tic():
    """
    对应MATLAB中的tic
    :return:
    """
    globals()['tt'] = time.clock()


def toc():
    """
    对应MATLAB中的toc
    :return:
    """
    t = time.clock() - globals()['tt']
    print('\nElapsed time: %.8f seconds\n' % t)
    return t


def yyyyMMddHHmm_2_datetime(dt):
    """
    输入一个长整型yyyyMMddhmm，返回对应的时间
    :param dt:
    :return:
    """
    dt = int(dt)  # FIXME：在python2下会有问题吗？
    (yyyyMMdd, hh) = divmod(dt, 10000)
    (yyyy, MMdd) = divmod(yyyyMMdd, 10000)
    (MM, dd) = divmod(MMdd, 100)
    (hh, mm) = divmod(hh, 100)

    return datetime(yyyy, MM, dd, hh, mm)


def yyyyMMdd_2_datetime(dt):
    yyyyMMdd = int(dt)
    (yyyy, MMdd) = divmod(yyyyMMdd, 10000)
    (MM, dd) = divmod(MMdd, 100)

    return datetime(yyyy, MM, dd, 0, 0)


def datetime_2_yyyyMMdd____(dt):
    """
    将时间转换成float类型
    :param dt:
    :return:
    """
    t = dt.timetuple()
    return float((t.tm_year * 10000.0 + t.tm_mon * 100 + t.tm_mday) * 10000.0)

def datetime_2_yyyyMMdd(dt):
    """
    将时间转换成float类型
    :param dt:
    :return:
    """
    t = dt.timetuple()
    return int((t.tm_year * 10000.0 + t.tm_mon * 100 + t.tm_mday))


def datetime_2_MM(dt):
    """
    将时间转换成float类型
    :param dt:
    :return:
    """
    t = dt.timetuple()
    return t.tm_mon


def datetime_2_yyyy(dt):
    """
    将时间转换成float类型
    :param dt:
    :return:
    """
    t = dt.timetuple()
    return t.tm_year


def datetime_2_yyyyMMddHHmm(dt):
    """
    将时间转换成float类型
    :param dt:
    :return:
    """
    t = dt.timetuple()
    return float((t.tm_year * 10000.0 + t.tm_mon * 100 + t.tm_mday) * 10000.0) + t.tm_hour * 100 + t.tm_min


def datetime_keep_yyyyMMdd(dt):
    """
    由于万得取出来时间有会带一个0500的小尾巴
    这个在处理数据时可能会导致问题，所以处理一下
    :return:
    """
    t = dt.timetuple()
    return datetime(t.tm_year, t.tm_mon, t.tm_mday, 0, 0)
