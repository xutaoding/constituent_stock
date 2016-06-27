# -*- coding: utf-8 -*-
from datetime import datetime
from collections import defaultdict
from pymongo import MongoClient

from conf import HOST, PORT, DB, COLLECTION


class IndexOutValidation(object):
    def __init__(self, query_date=None):
        if query_date is not None:
            self.in_dt = query_date.replace('-', '')
            query_date = [int(_) for _ in query_date.split('-')]
            self.query = {
                'upt': {
                    '$gte': datetime(*query_date + [0, 0, 0]),
                    '$lte': datetime(*query_date + [23, 59, 59])
                },
            }
        else:
            now = datetime.now()
            self.in_dt = now.strftime('%Y-%m-%d')
            self.query = {
                'upt': {
                    '$gte': datetime(now.year, now.month, now.day),
                    '$lte': datetime(now.year, now.month, now.day, 23, 59, 59)
                },
            }
        self.client = MongoClient(HOST, PORT)
        self.collection = self.client[DB][COLLECTION]

    def get_ordered_docs(self):
        total_data = defaultdict(list)
        fields = {'in_dt': 1, 'out_dt': 1, 'upt': 1, 'p_code': 1, 's_code': 1}

        for docs in self.collection.find({}, fields):
            key = docs['p_code'] + docs['s_code']
            total_data[key].append(docs)

        for _key, values_list in total_data.iteritems():
            values_list.sort(key=lambda _docs: _docs['in_dt'])
        return total_data

    def test_out_index(self):
        total_data = self.get_ordered_docs()

        print self.query
        print self.collection.find(self.query).count()
        for docs in self.collection.find(self.query):
            key = docs['p_code'] + docs['s_code']
            in_dt = docs['in_dt']

            for cmp_docs in total_data[key]:
                cmp_out_dt = cmp_docs['out_dt']

                # 同一天既做纳入， 又做剔除，两条记录
                if in_dt == cmp_out_dt:
                    print '"p_code": "%s", "s_code": "%s"' % (docs['p_code'], docs['s_code'])
        self.client.close()


if __name__ == '__main__':
    _query_date = '2016-06-24'
    IndexOutValidation(_query_date).test_out_index()

