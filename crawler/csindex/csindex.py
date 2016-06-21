# -*- coding: utf-8 -*-
import re
import sys
from datetime import datetime
from os.path import dirname, abspath

import pandas
import scrapy

sys.path.append(dirname(dirname(dirname(abspath(__file__)))))

from utils import StorageMongo
from utils.ftp import Ftp
from conf import logger
reload(sys)

sys.setdefaultencoding("utf-8")

total_data = []


class CsindexSpider(scrapy.Spider):
    name = "csindex"
    download_delay = 0.4
    start_urls = (
        "http://www.csindex.com.cn/sseportal/csiportal/xzzx/queryindexdownloadlist.do?type=1",
        "http://www.csindex.com.cn/sseportal/csiportal/xzzx/queryindexdownloadlist.do?type=2"
    )
    """
    字段s: 指数的简称 该字段值必须有
    字段p_code: 指数的代码 该字段值必须有
    字段s_code: 相应的指数下面对应的公司代码 该字段值必须有
    字段in_dt: 公司被纳入该指数的日期 该字段值必须有 ‘20160603’
    字段out_dt:公司被剔除该指数的日期 该字段值必须可选择有的话如‘20160603’没有为None
    字段sign: 公司是否被剔除该指数的标志 该字段值必须有默认为0 ‘0’ or ‘1’
    字段 cat: 从哪里抓取的数据分类 该字段值必须有
    字段ct: 该记录创建的时间 ‘20160602094201’
    """
    def parse(self, response):
        global total_data
        comp = re.compile("ftp://(.+)/(.+[.].+)")

        for table in response.css("table"):
            for tr in table.css("tr")[1:]:
                name = tr.css("td:nth-child(1) a::text").extract_first()
                uri = tr.css("td:nth-child(7) a::attr(href)").extract_first()
                if not uri or not comp.findall(uri):
                    continue

                today=datetime.now()
                host, file = comp.findall(uri)[0]
                ftp = Ftp(host)
                data = pandas.DataFrame()

                sheet_name, downloaded = ftp.download(file, "datas")
                if downloaded.empty or not downloaded.__contains__(u"成分券代码\nConstituent Code"):
                    continue
                try:
                    in_dt=datetime.strptime(sheet_name,"%Y%m%d")
                except ValueError:
                    in_dt=today

                def fmt_s_code(x):
                    if isinstance(x,(float,int, long)):
                        return "%06d"%x
                    else:
                        return x

                data["s_code"] = downloaded[u"成分券代码\nConstituent Code"].apply(fmt_s_code)
                data["p_abbr"] = name
                data["p_code"] = str(file[:-8])
                data["in_dt"] = in_dt.strftime("%Y%m%d")
                data["out_dt"] = None
                data["sign"] = '0'
                data["cat"] = self.name
                data["crt"] = today
                data["upt"] = today

                total_data.extend([row.to_dict() for ix, row in data.iterrows()])

    @staticmethod
    def close(spider, reason):
        closed = getattr(spider, 'closed', None)
        if callable(closed):
            return closed(reason)

        mongo = StorageMongo('csindex')
        logger.info(' Total_data count: <{}>'.format(len(total_data)))

        mongo.insert2mongo(total_data)
        mongo.eliminate()
        mongo.close()
        del total_data[:]


if __name__ == "__main__":
    from scrapy.crawler import CrawlerProcess

    cp = CrawlerProcess()
    cp.crawl(CsindexSpider())
    cp.start()
