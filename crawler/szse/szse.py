# -*- coding:utf-8 -*-
from __future__ import unicode_literals
import re
import sys
import time
from datetime import datetime
from random import randint, random
from multiprocessing.dummy import Pool as ThreadPool

from os.path import abspath, dirname
sys.path.append(dirname(dirname(dirname(abspath(__file__)))))

from utils import StorageMongo, HtmlLoader


class SzseIndex(HtmlLoader):
    category = 'szse'

    def __init__(self):
        self.base_url = 'http://www.szse.cn/szseWeb/FrontController.szse?randnum={r}&ACTIONID=7&' \
                        'AJAX=AJAX-TRUE&CATALOGID=1812&TABKEY=tab1&tab1PAGENUM={p}&tab1PAGECOUNT=17&' \
                        'tab1RECORDCOUNT=167&REPORT_ACTION=navigate'
        self.page_rule = re.compile(r'<td align="left" width="128px">(.*?)</td>', re.S)
        self.code_rule = re.compile(r"style='mso-number-format:\\@'.*?<u>(\d+?)</u>", re.S)
        self.mongo = StorageMongo(self.category)

    def get_total_pages(self):
        url = 'http://www.szse.cn/main/marketdata/hqcx/zsybg/'

        try:
            html = self.get_html(url)
            page_html = self.page_rule.findall(html)[0]
            pages_list = re.compile(r'\d+', re.S).findall(page_html)
            return int(pages_list[-1])
        except (IndexError, ValueError):
            pass
        return 17

    def signal_thread(self, p_code):
        index_code_rule = re.compile(r'mso-number-format:\\@.*?>(\d+?)</td>', re.S)
        base_url = 'http://www.szse.cn/szseWeb/ShowReport.szse?' \
                   'SHOWTYPE=EXCEL&CATALOGID=1747&ZSDM={code}&tab1PAGENUM=1&ENCODE=1&TABKEY=tab1'

        try:
            html = self.get_raw_html(base_url.format(code=p_code))
            time.sleep(1.1)
            code_list = index_code_rule.findall(html)

            for each_code in code_list:
                s_code = '0' * (6 - len(each_code)) + each_code

                self.mongo.insert2mongo({
                    "p_abbr": '',
                    "p_code": p_code,
                    "s_code": s_code,
                    "in_dt": time.strftime('%Y%m%d'),
                    "out_dt": None,
                    "sign": "0",
                    "cat": self.category,
                    "crt": datetime.now(),
                    'upt': datetime.now(),
                })
        except Exception as e:
            self.logger.info('SZSE crawl error: type <{}>, msg <{}>'.format(e.__class__, e))

    def crawl(self):
        total_pages = self.get_total_pages()
        pool = ThreadPool(10)

        for page in range(1, total_pages + 1):
            ran = str(random())
            r = ran + '0' * (18 - len(ran))
            html = self.get_raw_html(self.base_url.format(r=r, p=page))
            codes = self.code_rule.findall(html)

            pool.map(lambda _code: self.signal_thread(_code), codes)
            self.logger.info('SZSE Page <{}> crawl, Total page <{}> Done!'.format(page, total_pages))

            wait_time = randint(4, 8)
            time.sleep(wait_time)

        pool.close()
        pool.join()
        self.mongo.eliminate()
        self.mongo.close()


if __name__ == '__main__':
    print SzseIndex().crawl()

