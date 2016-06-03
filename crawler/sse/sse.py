import re
import time
import json
from datetime import datetime

from utils import HtmlLoader, StorageMongo
from conf import sse_config


class SSEIndex(HtmlLoader):
    def __init__(self):
        self.headers = sse_config['headers']
        self.base_url = sse_config['base_url']
        self.url = sse_config['ind_url'].format(t=str(time.time()).replace('.', ''))

        self.mongo = StorageMongo()

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

    def get_name_code_by_index(self):
        data_key = 'result'
        name_key = 'name'
        code_key = 'indexCode'
        raw_html = self.get_html(self.url, headers=self.headers)

        return [[item[name_key], item[code_key]] for item in self.unpickle(raw_html, data_key)]

    def crawl(self):
        data_key = 'result'
        name_codes = self.get_name_code_by_index()

        for name, code in name_codes:
            url = self.base_url.format(c=code, t=str(time.time()).replace('.', ''))
            html = self.get_html(url, headers=self.headers)

            for s_code, st, dt in self.unpickle(html, data_key):

                data = {
                    's': name, 'p_code': code, 's_code': s_code, 'in_dt': dt, 'out_dt': None, 'sign': '0',
                    'cat': 'sse', 'ct': re.compile(r'\s+|[-:\.]').sub('', str((datetime.now())))[:14]
                }
                self.mongo.insert2mongo(data)
            self.logger.info('Index <{name}>, Code <{code}> crawl spider!'.format(name=name, code=code))
            break


if __name__ == '__main__':
    SSEIndex().crawl()


