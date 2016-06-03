from __future__ import unicode_literals
import re
import time
import json
from datetime import datetime

from pymongo import MongoClient

from utils import HtmlLoader
from utils.util import get_md5
from conf import sse_config
from conf import A_HOST, A_PORT, A_DB, A_COLLECTION


class SSEAStock(HtmlLoader):
    def __init__(self):
        self.base_url = sse_config['a_url']
        self.headers = sse_config['a_headers']

        self.client = MongoClient(A_HOST, A_PORT)
        self.collection = self.client[A_DB][A_COLLECTION]

        self.cache = self._cached

    @property
    def _cached(self):
        fields = {'uid': 1}
        return {d['uid'] for d in self.collection.find({}, fields)}

    @staticmethod
    def unpickle(data, key=None):
        try:
            to_python = json.loads(data)

            if key is not None:
                return to_python[key]
            return to_python
        except Exception:
            pass
        return []

    def insert2mongo(self, docs):
        try:
            uid = docs['uid']

            if uid not in self.cache:
                self.collection.insert(docs)
        except Exception as e:
            self.logger.info('Insert mongo error: type <{}>, msg <{}>'.format(e.__class__, e))

    def crawl(self):
        start_page = 1
        total_page = 100
        data_key = 'result'

        while start_page <= total_page:
            url = self.base_url.format(p=start_page, t=str(time.time()).replace('.', ''))
            response = self.get_html(url, self.headers)
            to_python = self.unpickle(response, data_key)

            for item in to_python:
                item['uid'] = get_md5(''.join(item.values()))
                item['ct'] = re.compile(r'\s+|[-:\.]').sub('', str((datetime.now())))[:14]
                self.insert2mongo(item)

            self.logger.info('Crawl A stock ok: page <{}>'.format(start_page))

            if not to_python:
                break
            start_page += 1


if __name__ == '__main__':
    SSEAStock().crawl()


