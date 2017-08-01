#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
调用wset函数的部分

下载数据的方法
1.在时间上使用折半可以最少的下载数据，但已经下了一部分，要补下时如果挪了一位，又得全重下
2.在文件上，三个文件一组，三组一样，删中间一个，直到不能删了，退出
"""
import os
import pandas as pd
from datetime import datetime

from ..wind.wset import write_sectorconstituent, read_sectorconstituent, download_sectorconstituent


def _binsearch_download_sectorconstituent(w, dates, path, file_ext, start, end, sector, windcode, field,
                                          two_sides=False):
    """
    使用折半法下载
    缺点，数据都下载过来了，中间很多重复的数据
    :param w:
    :param dates:
    :param path:
    :param file_ext:
    :param start:
    :param end:
    :param sector:
    :param windcode:
    :param field:
    :param two_sides:
    :return:
    """
    len = end - start
    if len < 1:
        # 两个相邻，也需要进行操作
        return

    date = dates[start]
    fullpath = os.path.join(path, date + file_ext)
    df_start = read_sectorconstituent(fullpath)
    if df_start is None:
        print("下载:%s" % date)
        df_start = download_sectorconstituent(w, date, sector, windcode, field)
        write_sectorconstituent(fullpath, df_start)

    date = dates[end]
    fullpath = os.path.join(path, date + file_ext)
    df_end = read_sectorconstituent(fullpath)
    if df_end is None:
        print("下载:%s" % date)
        df_end = download_sectorconstituent(w, date, sector, windcode, field)
        write_sectorconstituent(fullpath, df_end)

    if two_sides:
        # 只处理两头，中间不管
        return

    # 靠近的，两边不同，又靠近的，那就输出就可以了
    if len <= 1:
        return

    set_start = set(df_start['wind_code'])
    set_end = set(df_end['wind_code'])
    if set_start == set_end:
        return
    print("%s %s" % (dates[start], dates[end]))
    print((set_start | set_end) - (set_start & set_end))

    # start,end两位置是否存在
    # 存在后检查两个数据是否一样，一样就跳过
    # 不一样就折半
    mid = (start + end) // 2
    _binsearch_download_sectorconstituent(w, dates, path, file_ext, start, mid, sector, windcode, field)
    _binsearch_download_sectorconstituent(w, dates, path, file_ext, mid, end, sector, windcode, field)


def file_download_sectorconstituent(w, dates, path, file_ext, sector, windcode, field):
    """
    输入目录和时间段，自动下载两边数据，同时补上中间数据，可能有重复，需要再清理
    :param w:
    :param dates:
    :param path:
    :param file_ext:
    :param sector:
    :param windcode:
    :param field:
    :return:
    """
    # 先为两条加上两个文件，然后使用文件遍历的算法
    _binsearch_download_sectorconstituent(w, dates, path, file_ext, 0, len(dates) - 1, sector,
                                          windcode, field,
                                          True)

    last_filename = None
    last_set = None
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            curr_df = read_sectorconstituent(filepath)
            curr_set = set(curr_df['wind_code'])
            if last_set is None:
                last_set = curr_set
                last_filename = filename
            else:
                if last_set == curr_set:
                    pass
                else:
                    part_dates = dates[last_filename[:-4]:filename[:-4]]
                    # 把前后两个数据之间的全下载过来，以后再考虑别的问题
                    _binsearch_download_sectorconstituent(w, part_dates, path, file_ext, 0, len(part_dates) - 1, sector,
                                                          windcode, field)
                last_set = curr_set
                last_filename = filename


def remove_sectorconstituent(path):
    """
    移除多余的数据，三个一组，重复的就删除中间那个
    保留了两侧，这样在下载数据时可以检查，发现一样就不操作了
    :return:
    """
    df_list = []
    path_list = []
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            curr_df = read_sectorconstituent(filepath)
            path_list.append(filepath)
            df_list.append(set(curr_df['wind_code']))

    # 分三个一组
    while True:
        print('=======开始一轮======')
        path_list_3 = [path_list[i:i + 3] for i in range(0, len(path_list), 3)]
        df_list_3 = [df_list[i:i + 3] for i in range(0, len(df_list), 3)]
        path_list = []
        df_list = []
        IsRemove = False
        for i, j in enumerate(df_list_3):
            if len(j) == 3:
                if j[0] == j[2]:
                    print("删除中间:%s" % path_list_3[i][1])
                    os.remove(path_list_3[i][1])
                    path_list.append(path_list_3[i][0])
                    path_list.append(path_list_3[i][2])
                    df_list.append(df_list_3[i][0])
                    df_list.append(df_list_3[i][2])
                    IsRemove = True
                else:
                    print("保留中间:%s" % path_list_3[i][1])
                    print("戴帽:%s" % ((j[0] | j[2]) - j[0]))
                    print("摘帽:%s" % ((j[0] | j[2]) - j[2]))
                    path_list.extend(path_list_3[i])
                    df_list.extend(df_list_3[i])
            else:
                path_list.extend(path_list_3[i])
                df_list.extend(df_list_3[i])

        if not IsRemove:
            print('Done')
            break

    print(path_list)


def download_sectors_list(
        w,
        root_path,
        sector_name="中信证券一级行业指数"):
    """
    下载行业分类列表
    :param w:
    :param sector_name:
    :param root_path:
    :return:
    """
    date_str = datetime.today().strftime('%Y-%m-%d')

    df = download_sectorconstituent(w, date_str, sector_name, None, 'wind_code')
    df['ID'] = list(range(0, df.shape[0]))
    df['ID'] += 1001

    path = os.path.join(root_path, '%s.csv' % sector_name)
    df.to_csv(path, encoding='utf-8-sig', date_format='%Y-%m-%d', index=None)
    return df


def download_sectors(
        w,
        trading_days,
        root_path,
        sector_name="中信证券一级行业指数"):
    """
    指定行业列表后，下载其中的数据，带子目录
    :param w:
    :param trading_days:
    :param sector_name:
    :param root_path:
    :return:
    """
    # 下载板块数据
    path = os.path.join(root_path, '%s.csv' % sector_name)
    sectors = pd.read_csv(path, encoding='utf-8-sig')

    for i in range(0, len(sectors)):
        print(sectors.iloc[i, :])

        wind_code = sectors.ix[i, 'wind_code']
        sec_name = sectors.ix[i, 'sec_name']

        foldpath = os.path.join(root_path, sector_name, sec_name)
        try:
            os.mkdir(foldpath)
        except:
            pass

        df = trading_days
        df['date_str'] = trading_days['date'].astype(str)
        file_download_sectorconstituent(w, df['date_str'], foldpath, '.csv',
                                        sector=None, windcode=wind_code, field='wind_code')
        # 移除多余的数据文件
        remove_sectorconstituent(foldpath)


def download_sector(
        w,
        trading_days,
        root_path,
        sector_name="风险警示股票"):
    """
    下载ST股票的信息，在已有的文件中补数据，这种不会多下载
    :param w:
    :param trading_days:
    :param sector_name:
    :param root_path:
    :return:
    """
    df = trading_days
    df['date_str'] = trading_days['date'].astype(str)

    foldpath = os.path.join(root_path, sector_name)
    file_download_sectorconstituent(w, df['date_str'], foldpath, '.csv',
                                    sector=sector_name, windcode=None, field='wind_code')
    remove_sectorconstituent(foldpath)
