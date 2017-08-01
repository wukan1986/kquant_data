#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
大智慧数据的处理
"""
import urllib
import urllib.request
import numpy as np
from struct import *

from ..xio.h5 import write_dataframe_set_struct_keep_head

dzh_h5_type = np.dtype([
    ('time', np.uint64),
    ('pre_day', np.float64),
    ('pre_close', np.float64),
    ('split', np.float64),
    ('purchase', np.float64),
    ('purchase_price', np.float64),
    ('dividend', np.float64),
    ('dr_pre_close', np.float64),
    ('dr_factor', np.float64),
    ('backward_factor', np.float64),
    ('forward_factor', np.float64),
])


def dividend_to_h5(input_path, data):
    write_dataframe_set_struct_keep_head(input_path, data, dzh_h5_type, 'Dividend')
    return


class DzhFetcher(object):
    _IPS = ('222.73.103.181', '222.73.103.183')
    _PATH = None
    _FILE_PATH = None

    def __init__(self, filepath=None):
        self.ips = list(self._IPS)
        self._fetched = False
        self._FILE_PATH = filepath

    def fetch_next_server(self):
        self.ips.pop
        if len(self.ips) == 0:
            raise FileNotFoundError
        return self.fetch()

    def fetch(self):
        if self._FILE_PATH is None:
            return self._fetch_url()
        else:
            return self._fetch_file()

    def _fetch_url(self):
        try:
            r = urllib
            data = r.read()
            self.f = xio.StringIO(data)
            self._fetched = True
        except urllib.URLError:
            return self.fetch_next_server()

    def _fetch_file(self):
        try:
            self.f = open(self._FILE_PATH, 'rb')
            self._fetched = True
        except OSError as e:
            raise e

    def data_url(self):
        assert self._PATH, "No file path."

        if len(self.ips) == 0:
            return None

        return "http://" + self.ips[-1] + self._PATH


class DzhDividend(DzhFetcher):
    """大智慧除权数据"""
    _PATH = '/platform/download/PWR/full.PWR'

    def read(self):
        """Generator of 大智慧除权数据

        Example of yield data:

        symbol: 'SZ000001'
        dividends: [{ :date_ex_dividend => '1992-03-23',
                      :split => 0.500,
                      :purchase => 0.000,
                      :purchase_price => 0.000,
                      :dividend => 0.200 }... ]
        """
        if not self._fetched:
            self.fetch()

        # skip head
        self.f.seek(12, 0)

        try:
            while True:
                yield self._read_symbol()
        except EOFError:
            raise StopIteration
        finally:
            self.f.close()
            # except Exception as e:
            #    print(e)

    def _read_symbol(self):
        dividends = []

        rawsymbol = self.f.read(16)
        if rawsymbol == b'':
            raise EOFError

        symbol = unpack('16s', rawsymbol)[0].replace(b'\x00', b'')

        rawdate = self.f.read(4)

        dt = np.dtype([('time', np.int32),
                       ('split', np.float32),
                       ('purchase', np.float32),
                       ('purchase_price', np.float32),
                       ('dividend', np.float32)])
        while (rawdate) != b"\xff" * 4:
            dividend = np.frombuffer(rawdate + self.f.read(16), dtype=dt)
            dividends.append(dividend)

            rawdate = self.f.read(4)
            if rawdate == b'':
                break

        return (symbol, np.fromiter(dividends, dtype=dt))


def download_pwr(
        local_file=r"D:\dzh2\Download\PWR\full.PWR",
        url='http://222.73.103.181/platform/download/PWR/full.PWR',
        proxy=None):
    if proxy is not None:
        # create the object, assign it to a variable
        proxy = urllib.request.ProxyHandler(proxy)  # {'http': '192.168.1.60:808'}
        # construct a new opener using your proxy settings
        opener = urllib.request.build_opener(proxy)
        # install the openen on the module-level
        urllib.request.install_opener(opener)

    # 这里需要处理一下，除权信息已经没法直接下载了
    f = urllib.request.urlopen(url)
    data = f.read()
    with open(local_file, "wb") as code:
        code.write(data)

    print(u'下载除权除息信息完成')

