# -*- coding:utf-8 -*-
from __future__ import unicode_literals
import lxml.html
import re
import requests
import chardet
import time
from utils import StorageMongo, HtmlLoader
from conf import logger


class SZSEIndex(object):
    def __init__(self):
        self.index_url = r'http://www.szse.cn/main/marketdata/hqcx/zsybg/'
        self.ajax_url = r'http://www.szse.cn/szseWeb/FrontController.szse'
        self.header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/49.0.2623.110 Safari/537.36'
        }
        self._count = re.compile(r'\d+', re.S)
        self._date = re.compile(r'\d+-.*', re.S)
        self.mongo = StorageMongo()

    def covert_charset(self, string):
        charset = chardet.detect(string)['encoding']  # return value is a dictionary(have a key is 'encoding')
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
            resp = requests.post(self.ajax_url, data=data, headers=self.header, timeout=30)
            if resp.status_code != 200:
                return
        else:
            resp = requests.get(self.index_url)
        tree = lxml.html.fromstring(resp.content)
        counts = tree.xpath('//td[@align="left"][@width="128px"]/text()')
        if counts:
            return int(self._count.findall(''.join(counts))[1])

    def upload(self):
        counts = self.page_count()

        for page in range(1, counts + 1):
            data = {'AJAX': 'AJAX-TRUE', 'TABKEY': 'tab1', 'ACTIONID': 7, 'tab1PAGENUM': page, 'CATALOGID': 1812}
            # resp = requests.post(self.ajax_url, data=data, headers=self.header, timeout=30)
            # raw_html = self.covert_charset(resp.content)
            raw_html = self.covert_charset(HtmlLoader().get_raw_html(self.ajax_url, data=data))
            tree = lxml.html.fromstring(unicode(raw_html, 'utf-8'))
            index_names = tree.xpath('//td[@class="cls-data-td"][@style="mso-number-format:\@"]/a/u/text()')

            for p in index_names:
                ajax_counts = self.page_count({'ZSDM': p, 'TABKEY': 'tab1', 'ACTIONID': 7, 'CATALOGID': 1747})
                if not ajax_counts:
                    continue
                for u_page in range(1, ajax_counts + 1):
                    data_1 = {'ZSDM': p, 'TABKEY': 'tab1', 'ACTIONID': 7, 'CATALOGID': 1747, 'tab1PAGENUM': u_page}
                    # r = requests.post(self.ajax_url, data=data_1)
                    # html = self.covert_charset(r.content)
                    html = self.covert_charset(HtmlLoader().get_raw_html(self.ajax_url, data=data_1))
                    etree = lxml.html.fromstring(unicode(html, 'utf-8'))
                    _date = etree.xpath('//span[@class="cls-subtitle"]/text()')
                    in_dt = re.sub('-', '', str(self._date.findall(''.join(_date))[0]))
                    if in_dt == '':
                        in_dt = time.strftime('%Y%m%d')
                    name = _date[0].split('  ')[1]
                    codes = etree.xpath('//td[@style="mso-number-format:\@"]/text()')

                    for code in codes:
                        # logger.info("Index name:%s,p_code:%s,s_code:%s,date:%s \n" % (name, p, code, in_dt))
                        self.mongo.insert2mongo({
                            "s": name,
                            "p_code": p,
                            "s_code": code,
                            "in_dt": in_dt,
                            "out_dt": None,
                            "sign": "0",
                            "cat": "szse",
                            "ct": time.strftime('%Y%m%d%H%M%S')
                        })
                    logger.info('Pages:[%d], %s page [%s]' % (page, u_page, name))
                    time.sleep(0.4)
                time.sleep(5)
        self.mongo.close()

if __name__ == '__main__':
    import time
    st = time.time()
    SZSEIndex().upload()
    print time.time() - st



