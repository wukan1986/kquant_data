#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
调用wset函数的部分

下载数据的方法
1.在时间上使用折半可以最少的下载数据，但已经下了一部分，要补下时如果挪了一位，又得全重下
2.在文件上，三个文件一组，三组一样，删中间一个，直到不能删了，退出
"""
import os
import shutil
import pandas as pd
from datetime import datetime

from ..wind.wset import write_constituent, read_constituent, download_sectorconstituent, download_indexconstituent, \
    download_futureoir


def _binsearch_download_constituent(w, dates, path, file_ext, start, end, sector, windcode, field,
                                    two_sides=False, is_indexconstituent=False):
    """
    使用折半法下载
    缺点，数据都下载过来了，中间很多重复的数据
    :param w:
    :param dates:
    :param path:
    :param file_ext:
    :param start:
    :param end:
    :param sector:None表示没有权块信息，那就是权重信息了
    :param windcode:
    :param field:
    :param two_sides:用在一开始数据下载时补两头
    :return:
    """
    len = end - start
    if len < 1:
        # 两个相邻，也需要进行操作
        return

    date = dates[start]
    fullpath = os.path.join(path, date + file_ext)
    df_start = read_constituent(fullpath)
    if df_start is None:
        print("下载:%s" % date)
        if not is_indexconstituent:
            df_start = download_sectorconstituent(w, date, sector, windcode, field)
        else:
            df_start = download_indexconstituent(w, date, windcode, field)
        write_constituent(fullpath, df_start)

    date = dates[end]
    fullpath = os.path.join(path, date + file_ext)
    df_end = read_constituent(fullpath)
    if df_end is None:
        print("下载:%s" % date)
        if not is_indexconstituent:
            df_end = download_sectorconstituent(w, date, sector, windcode, field)
        else:
            df_end = download_indexconstituent(w, date, windcode, field)
        write_constituent(fullpath, df_end)

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
    _binsearch_download_constituent(w, dates, path, file_ext, start, mid, sector, windcode, field, two_sides,
                                    is_indexconstituent)
    _binsearch_download_constituent(w, dates, path, file_ext, mid, end, sector, windcode, field, two_sides,
                                    is_indexconstituent)


def file_download_constituent(w, dates, path, file_ext, sector, windcode, field, is_indexconstituent):
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
    _binsearch_download_constituent(w, dates, path, file_ext, 0, len(dates) - 1, sector,
                                    windcode, field,
                                    True, is_indexconstituent)

    last_filename = None
    last_set = None
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            curr_df = read_constituent(filepath)
            if curr_df is None:
                continue
            curr_set = set(curr_df['wind_code'])
            if last_set is None:
                last_set = curr_set
                last_filename = filename
            else:
                if last_set == curr_set:
                    print(filepath)
                    pass
                else:
                    part_dates = dates[last_filename[:-4]:filename[:-4]]
                    # 把前后两个数据之间的全下载过来，以后再考虑别的问题
                    _binsearch_download_constituent(w, part_dates, path, file_ext, 0, len(part_dates) - 1, sector,
                                                    windcode, field, False, is_indexconstituent)
                last_set = curr_set
                last_filename = filename


def move_constituent(path, dst_path):
    """
    移除多余的数据，三个一组，重复的就删除中间那个
    保留了两侧，这样在下载数据时可以检查，发现一样就不操作了
    三个一组，第4个和第5个的重复无法检测
    :return:
    """
    df_list = []
    path_list = []
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            curr_df = read_constituent(filepath)
            path_list.append(filepath)
            df_list.append(set(curr_df['wind_code']))

    # 分三个一组
    k = 0
    while True:
        print('=======开始一轮======', k)

        path_list_first = path_list[0:k % 3]
        df_list_first = df_list[0:k % 3]
        # 有可能出现第三个与第四个一样，但无法排除的问题，所以用k来移动一下
        path_list_3 = [path_list[i:i + 3] for i in range(k % 3, len(path_list), 3)]
        df_list_3 = [df_list[i:i + 3] for i in range(k % 3, len(df_list), 3)]
        path_list = []
        df_list = []
        # 移动时会出现前部分没有在循环中，被抛弃，所以要主动添加进来
        path_list.extend(path_list_first)
        df_list.extend(df_list_first)
        IsRemove = False
        for i, j in enumerate(df_list_3):
            if len(j) == 3:
                if j[0] == j[2]:
                    print("移除中间:%s" % path_list_3[i][1])
                    src_file = os.path.split(path_list_3[i][1])
                    dst_file = os.path.join(dst_path, src_file[1])
                    if os.path.exists(dst_file):
                        os.remove(dst_file)
                    shutil.move(path_list_3[i][1], dst_path)
                    path_list.append(path_list_3[i][0])
                    path_list.append(path_list_3[i][2])
                    df_list.append(df_list_3[i][0])
                    df_list.append(df_list_3[i][2])
                    IsRemove = True
                else:
                    # print("保留中间:%s" % path_list_3[i][1])
                    # print("戴帽:%s" % ((j[0] | j[2]) - j[0]))
                    # print("摘帽:%s" % ((j[0] | j[2]) - j[2]))
                    path_list.extend(path_list_3[i])
                    df_list.extend(df_list_3[i])
            else:
                path_list.extend(path_list_3[i])
                df_list.extend(df_list_3[i])

        if not IsRemove:
            print('此轮没有要移动的文件，查下一次')
            k += 1
            if k > 5:
                # 第三轮其实已经处理得差不多了
                break
                # print(path_list)


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
        file_download_constituent(w, df['date_str'], foldpath, '.csv',
                                  sector=None, windcode=wind_code, field='wind_code', is_indexconstituent=False)
        # 移除多余的数据文件
        move_constituent(foldpath)


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
    file_download_constituent(w, df['date_str'], foldpath, '.csv',
                              sector=sector_name, windcode=None, field='wind_code', is_indexconstituent=False)
    move_constituent(foldpath)


def download_index_weight(w, trading_days, wind_code, root_path):
    """
    下载指数成份和权重
    :param w:
    :param trading_days:
    :param wind_code:
    :param root_path:
    :return:
    """
    df = trading_days
    df['date_str'] = trading_days['date'].astype(str)

    foldpath = os.path.join(root_path, wind_code)
    file_download_constituent(w, df['date_str'], foldpath, '.csv',
                              sector=None, windcode=wind_code, field='wind_code,i_weight', is_indexconstituent=True)

    dst_path = os.path.join(root_path, "%s_move" % wind_code)
    move_constituent(foldpath, dst_path)


def download_index_weight2(w, dates, wind_code, root_path):
    for date in dates:
        path = os.path.join(root_path, wind_code, date.strftime('%Y-%m-%d.csv'))
        df = read_constituent(path)
        if df is None:
            print("下载权重", path)
            df = download_indexconstituent(w, date.strftime('%Y-%m-%d'), wind_code)
            write_constituent(path, df)


def download_futureoir_day_range(w, day_range, windcode, root_path):
    """
    指定
    :param w:
    :param day_range:
    :param windcode:
    :param root_path:
    :return:
    """
    startdate = day_range[0]
    enddate = day_range[-1]
    print("长度%d,开始%s,结束%s" % (len(day_range), startdate, enddate))
    if len(day_range) > 0:
        df = download_futureoir(w, startdate=startdate, enddate=enddate, windcode=windcode)
        if df.empty:
            return 0
        dfg = df.groupby(by=['date'])
        for name, group in dfg:
            try:
                # 如果数据超出，这里会报错
                path = os.path.join(root_path, windcode, name.strftime('%Y-%m-%d.csv'))
                # 索引不要保存
                group.to_csv(path, encoding='utf-8-sig', date_format='%Y-%m-%d', index=False)
            except:
                print('下载出错，可能需要重新运行一次')
        return len(day_range)
    return 0


def resume_download_futureoir(w, trading_days, root_path, windcode, adjust_trading_days):
    dir_path = os.path.join(root_path, windcode)
    os.makedirs(dir_path, exist_ok=True)

    # 从第一个有数据的部分开始第一条,这个需要根据自己的情况进行处理
    if adjust_trading_days:
        for dirpath, dirnames, filenames in os.walk(dir_path):
            for filename in filenames:
                trading_days = trading_days[filename[:10]:]
                break

    # 分成list套list，只要时间超长就分新的一组
    last_date = pd.Timestamp(1900, 1, 1)
    day_ranges = []
    day_range = []
    for day in trading_days['date']:
        path = os.path.join(root_path, windcode, day.strftime('%Y-%m-%d.csv'))
        if os.path.exists(path):
            continue
        else:
            if (day - last_date).days > 20 or len(day_range) >= 30:
                day_ranges.append(day_range)
                day_range = []
            last_date = day
            day_range.append(day)
    # 将最后一个添加进去
    day_ranges.append(day_range)

    for day_range in day_ranges:
        if len(day_range) > 0:
            ret = download_futureoir_day_range(w, day_range, windcode, root_path)
            if ret == 0:
                print("下载数据为空，后面不再处理")
                break

    print('处理完毕！可能有部分数据由于超时没有下载成功，可再运行一次脚本')
