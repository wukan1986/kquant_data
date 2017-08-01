#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

"""
import pandas as pd
from ..wind.tdays import read_tdays, download_tdays, write_tdays


def resume_download_tdays(w, enddate, path):
    """
    增量下载
    :return:
    """
    df_old = read_tdays(path)
    if df_old is None:
        startdate = '1991-01-01'
    else:
        startdate = df_old.index[-1]
    df_new = download_tdays(w, startdate, enddate, option="")
    df = pd.concat([df_old, df_new])

    # 可能要‘去重’，也可能None不能参与合并
    write_tdays(path, df)
