from __future__ import unicode_literals
import re
import sys
import time
import json
from random import randint
from datetime import datetime
from os.path import abspath, dirname
from multiprocessing.dummy import Pool as ThreadPool

sys.path.append(dirname(dirname(dirname(abspath(__file__)))))

from utils import HtmlLoader, StorageMongo
from conf import sse_config


class SSEIndex(HtmlLoader):
    category = 'sse'

    def __init__(self):
        self.headers = sse_config['headers']
        self.base_url = sse_config['base_url']
        self.url = sse_config['ind_url'].format(t=str(time.time()).replace('.', ''))

        self.mongo = StorageMongo(self.category)

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
        all_name_code = []
        raw_html = self.get_html(self.url, headers=self.headers)

        for item in self.unpickle(raw_html, data_key):
            name_code = item[name_key], item[code_key]

            # Remove JingDong don't include indexes
            if not self.validate_index(name_code[1]):
                continue

            if name_code not in all_name_code:
                all_name_code.append(name_code)
        return all_name_code

    def signal_thread(self, name, code):
        count = 0
        data_key = 'result'
        repl_dt = (lambda _dt: dt.replace('-', ''))
        url = self.base_url.format(c=code, t=str(time.time()).replace('.', ''))
        html = self.get_raw_html(url, headers=self.headers)
        time.sleep(0.3)

        for _index, item in enumerate(self.unpickle(html, data_key), 1):
            s_code, st, dt = item

            try:
                data = {
                    'p_abbr': name, 'p_code': code, 's_code': s_code, 'in_dt': repl_dt(dt), 'out_dt': None,
                    'sign': '0', 'cat': self.category, 'crt': datetime.now(), 'upt': datetime.now()
                }
                self.mongo.insert2mongo(data)
                count += 1
            except Exception as e:
                self.logger.info('SSE code <{} {}> site crawl error: typ <>, msg <>'.format(
                    code, s_code, type.__class__, e))
        # self.logger.info('Index <{name}>, Code <{code}>, Count <{count}> crawl spider!'.format(
        #         name=name, code=code, count=count))

    def crawl(self):
        thread_num = 10
        name_codes = self.get_name_code_by_index()
        length = len(name_codes) / thread_num + 1

        pool = ThreadPool(thread_num)
        iterable_args = [name_codes[i * thread_num: (i + 1) * thread_num] for i in range(length)]

        for i, arguments in enumerate(iterable_args, 1):
            pool.map(lambda args: self.signal_thread(*args), arguments)
            self.logger.info('SSE crawl page <{}>, Total Pages <{}> Done!'.format(i, length))
            wait_time = randint(3, 6)
            time.sleep(wait_time)

        pool.close()
        pool.join()
        self.mongo.eliminate()
        self.mongo.close()


if __name__ == '__main__':
    SSEIndex().crawl()


