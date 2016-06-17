# coding=utf-8
# author: shuqing.zhou
import os
import sys
import time
import datetime
from multiprocessing.dummy import Pool as ThreadPool
from urllib import urlretrieve

import xlrd
from bs4 import BeautifulSoup

from utils import HtmlLoader, StorageMongo
from utils.util import get_md5

reload(sys)
sys.setdefaultencoding("utf-8")


class CNIndex(object):
    category = 'cnindex'

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, sdch",
        "Accept-Language": "zh-CN,zh;q=0.8,en;q=0.6",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Host": "www.cnindex.com.cn",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": 1,
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
    }

    def __init__(self):
        self.mongo = StorageMongo()

    def parse_xls(self, path):
        data = xlrd.open_workbook(path)
        sh = data.sheet_by_index(0)
        s_line = None
        p_code_line = None
        s_code_line = None
        in_dt = None
        for i in range(0, sh.ncols):
            if u"指数简称" == sh.row_values(0)[i]:
                s_line = i
            if u"指数代码" == sh.row_values(0)[i]:
                p_code_line = i
            if u"证券代码" == sh.row_values(0)[i]:
                s_code_line = i
            if u"更新日期：" in sh.row_values(0)[i]:
                dt_tmp = str(sh.row_values(0)[i]).replace("更新日期：", "").strip()
                in_dt = datetime.datetime.strftime(datetime.datetime.strptime(dt_tmp, "%Y-%m-%d"), "%Y%m%d")
        if s_line is not None and p_code_line is not None and s_code_line is not None and in_dt is not None:
            for i in range(1, sh.nrows):
                p_code = None
                try:
                    p_code = str(int(sh.row_values(i)[p_code_line]))
                except ValueError:
                    p_code = str(sh.row_values(i)[p_code_line])
                s_code = str(int(sh.row_values(i)[s_code_line])).rjust(6, "0")
                self.mongo.insert2mongo({
                    "s": sh.row_values(i)[s_line],
                    "p_code": p_code,
                    "s_code": s_code,
                    "in_dt": in_dt,
                    "out_dt": None,
                    "sign": "0",
                    "cat": self.category,
                    "ct": time.strftime('%Y%m%d%H%M%S')
                })
        os.remove(path)

    def parse_detail(self, url):
        hl = HtmlLoader()
        html = hl.get_html(url=url, headers=self.headers)
        soup = BeautifulSoup(html, "lxml")
        for info in soup.select(".szzs_list li a"):
            if u"指数样本" == info.text:
                try:
                    file_url = "http://www.cnindex.com.cn/docs/" + info.get("href")[info.get("href").rfind("/") + 1:]
                    print file_url
                    file_name = get_md5(file_url[0:file_url.rfind(".")]) + file_url[file_url.rfind("."):]
                    urlretrieve(file_url, filename=file_name)
                    self.parse_xls(file_name)
                except Exception as e:
                    print e.message
                break

    def main(self):
        hl = HtmlLoader()
        html = hl.get_html(url="http://www.cnindex.com.cn/zstx/szxl/", headers=self.headers)
        soup = BeautifulSoup(html, "lxml")
        for tr in soup.select(".RightBox tr"):
            if tr.select("td"):
                if tr.select("td")[0].get("bgcolor") == "#FFFFFF":
                    url = "http://www.cnindex.com.cn/zstx/szxl/" + tr.select_one("td a").get("href")
                    self.parse_detail(url)


if __name__ == '__main__':
    a = time.time()
    cn = CNIndex()
    cn.main()
    print time.time() - a
