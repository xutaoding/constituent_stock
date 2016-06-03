HOST = '192.168.100.20'
PORT = 27017
DB = 'py_crawl'
COLLECTION = 'constituent'

A_HOST = '192.168.100.20'
A_PORT = 27017
A_DB = 'py_crawl'
A_COLLECTION = 'a_stock'

USER_AGENT = [
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.3319.102 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2309.372 Safari/537.36',

    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20130401 Firefox/31.0',
    'Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20120101 Firefox/29.0',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/29.0',

    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0b11pre) Gecko/20110128 Firefox/4.0b11pre',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b11pre) Gecko/20110131 Firefox/4.0b11pre',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b11pre) Gecko/20110129 Firefox/4.0b11pre',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b11pre) Gecko/20110128 Firefox/4.0b11pre',
    'Mozilla/5.0 (Windows NT 6.1; rv:2.0b11pre) Gecko/20110126 Firefox/4.0b11pre',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b10pre) Gecko/20110118 Firefox/4.0b10pre',
]

sse_config = {
    'base_url': 'http://query.sse.com.cn/index/consList.do?jsonCallBack=&indexCode={c}&_={t}',
    'ind_url': 'http://query.sse.com.cn/index/queryIndexDataList.do?jsonCallBack=&pageHelp.pageSize=100&_={t}',
    'headers': {
        'Host': 'query.sse.com.cn',
        'Referer': 'http://www.sse.com.cn/market/sseindex/indexlist/',
    },

    # Crawl A stock
    'a_url': 'http://query.sse.com.cn/security/stock/getStockListData2.do?&jsonCallBack=&isPagination=true&stockCode=&'
             'csrcCode=&areaName=&stockType=1&pageHelp.cacheSize=1&pageHelp.beginPage={p}&pageHelp.pageSize=25&'
             'pageHelp.pageNo={p}&pageHelp.endPage=21&_={t}',
    'a_headers': {
        'Host': 'query.sse.com.cn',
        'Referer': 'http://www.sse.com.cn/assortment/stock/list/share/'
    }

}



