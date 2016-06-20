# -*- coding:utf-8 -*-
from __future__ import unicode_literals
import lxml.html
import re
import sys
import time
import requests
import chardet
from random import randint
from multiprocessing.dummy import Pool as ThreadPool

from os.path import abspath, dirname
sys.path.append(dirname(dirname(dirname(abspath(__file__)))))

from utils import StorageMongo, HtmlLoader
from conf import logger


class SZSEPatch(object):
    category = 'szse'

    def __init__(self):
        self.index_url = r'http://www.szse.cn/main/marketdata/hqcx/zsybg/'
        self.ajax_url = r'http://www.szse.cn/szseWeb/FrontController.szse'
        self.header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/49.0.2623.110 Safari/537.36'
        }
        self.excel_url = r'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL' \
                         r'&CATALOGID=1747&ZSDM=%s' \
                         r'&tab1PAGENUM=1&ENCODE=1&TABKEY=tab1'
        self._scode = re.compile(r"<td  class='cls-data-td' style='mso-number-format:\\@' align='center' >(\d+)</td>",
                                 re.S)
        self._count = re.compile(r'\d+', re.S)
        self._date = re.compile(r'\d+-.*', re.S)
        self.mongo = StorageMongo(self.category)

    def parse_html(self, html):

        return self._scode.findall(html)

    def get_html(self, p_code):
        url = self.excel_url % p_code
        raw_html = self.covert_charset(HtmlLoader().get_raw_html(url))
        return raw_html

    def covert_charset(self, string):
        charset = chardet.detect(string)['encoding']
        if charset is None:
            return string
        if charset != 'utf-8' and charset == 'GB2312':
            charset = 'gb18030'
        try:
            return string.decode(charset).encode('utf-8')
        except Exception, e:
            print 'chardet error:', e
        return '<html></html>'

    def page_count(self, data=None):
        if data:
            html = self.covert_charset(HtmlLoader().get_raw_html(self.ajax_url, data=data, headers=self.header))
            tree = lxml.html.fromstring(unicode(html, 'utf-8'))
            counts = tree.xpath('//td[@align="left"][@width="128px"]/text()')
            _date = tree.xpath('//span[@class="cls-subtitle"]/text()')

            if len(_date) != 0:
                date = self._date.findall(''.join(_date))
                if len(date) != 0:
                    in_dt = re.sub('-', '', str(date[0]))
                    if not in_dt:
                        in_dt = time.strftime('%Y%m%d')
                name = _date[0].split('  ')[1]

            if counts:
                return int(self._count.findall(''.join(counts))[1]), in_dt, name
            else:
                return None, '', ''
        else:
            resp = requests.get(self.index_url)
            tree = lxml.html.fromstring(resp.content)
            counts = tree.xpath('//td[@align="left"][@width="128px"]/text()')
            if counts:
                return int(self._count.findall(''.join(counts))[1])

    def single_thread(self, p):
        try:
            ajax_counts, in_dt, name = self.page_count({'ZSDM': p, 'TABKEY': 'tab1', 'ACTIONID': 7, 'CATALOGID': 1747})
            if not ajax_counts:
                return

            html = self.get_html(p)
            s_code = self.parse_html(html)
            if s_code:
                for code in s_code:
                    self.mongo.insert2mongo({
                        "s": name,
                        "p_code": p,
                        "s_code": code,
                        "in_dt": in_dt,
                        "out_dt": None,
                        "sign": "0",
                        "cat": self.category,
                        "ct": time.strftime('%Y%m%d%H%M%S')
                    })
                logger.info('pcode:%s,name:%s' % (p, name))
        except Exception as e:
            logger.info('SZSe crawl error: type <{}>, msg <{}>'.format(e.__class__, e))

    def upload(self):
        counts = self.page_count()
        pool = ThreadPool(12)

        for page in range(1, counts + 1):
            data = {'AJAX': 'AJAX-TRUE', 'TABKEY': 'tab1', 'ACTIONID': 7, 'tab1PAGENUM': page, 'CATALOGID': 1812}
            raw_html = self.covert_charset(HtmlLoader().get_raw_html(self.ajax_url, data=data))
            tree = lxml.html.fromstring(unicode(raw_html, 'utf-8'))
            index_names = tree.xpath('//td[@class="cls-data-td"][@style="mso-number-format:\@"]/a/u/text()')

            pool.map(lambda _p: self.single_thread(_p), [p for p in index_names])
            wait_time = randint(3, 8)
            time.sleep(wait_time)

        pool.close()
        pool.join()
        self.mongo.eliminate()
        self.mongo.close()

if __name__ == '__main__':
    SZSEPatch().upload()




