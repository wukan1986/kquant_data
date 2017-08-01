#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

"""
from datetime import datetime, timedelta


def asDateTime(v, asDate=False):
    """
    万得中读出来的时间总多5ms，覆写这部分
    w.asDateTime = asDateTime
    w.start()
    :param v:
    :param asDate:
    :return:
    """
    # return datetime(1899, 12, 30, 0, 0, 0, 0) + timedelta(v + 0.005 / 3600 / 24)
    return datetime(1899, 12, 30, 0, 0, 0, 0) + timedelta(v)
