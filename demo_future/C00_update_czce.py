#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
更新郑商所合约名，3位转4位，不小心转错的需要再转回来
只在需要的时候才用，由于每天都在下数据，历史上的只要不下载历史数据就可以不更新
FG这种有12个月的可用来参考，然后改CZCE_convert函数
"""
import os
import pandas as pd
from kquant_data.future.symbol import CZCE_convert
from kquant_data.wind_resume.wset import read_constituent, write_constituent
from kquant_data.config import __CONFIG_H5_FUT_SECTOR_DIR__


def process_CZCE_type2(path):
    # 郑商所的合约，对于已经退市的合约需要将3位变成4位
    # 1. 最后一个文件肯定是最新的，它是当前正在交易的合约，应当全是3位，然后遍历其它文件没有出现的就可以改名
    # 2. 实际测试后发现，退市的合约并不是立即换成4位，需要等一年多，或更长时间
    for dirpath, dirnames, filenames in os.walk(path, topdown=False):
        for filename in filenames:
            if filename < '2015-01-01.csv':
                continue
            curr_path = os.path.join(dirpath, filename)
            print(curr_path)
            curr_df = read_constituent(curr_path)
            if curr_df is None:
                continue
            curr_set = set(curr_df['wind_code'])
            mod_set = set(map(CZCE_convert, curr_set))
            if curr_set == mod_set:
                continue

            new_df = pd.DataFrame(list(mod_set), columns=['wind_code'])
            new_df.sort_values(by=['wind_code'], inplace=True)
            write_constituent(curr_path, new_df)


if __name__ == '__main__':
    path = os.path.join(__CONFIG_H5_FUT_SECTOR_DIR__, '郑商所全部品种')
    process_CZCE_type2()
