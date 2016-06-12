# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import hashlib
from operator import itemgetter
from datetime import datetime, timedelta
from collections import defaultdict

from pymongo import MongoClient
from conf import HOST, PORT, DB, COLLECTION


def get_md5(value):
    if not isinstance(value, basestring):
        raise ValueError('md5 must string!')
    m = hashlib.md5()
    try:
        m.update(value)
    except UnicodeEncodeError:
        m.update(value.encode('u8'))
    return m.hexdigest()


def get_data_from_mongo():
    cached = {}
    client = MongoClient(HOST, PORT)
    collection = client[DB][COLLECTION]

    fields = ['p_code', 's_code', 'sign', 'ct']
    # Annotation: `sign` company be bring into related index with zero, else with 1
    cursor = collection.find({}, {k: 1 for k in fields}).sort([('in_dt', -1)])

    for docs in cursor:
        sign = int(docs['sign'])
        ps_code = docs['p_code'] + docs['s_code']
        value_dict = {'ct': docs['ct'], '_id': docs['_id']}
        cached[ps_code] = value_dict

        if sign:
            cached[ps_code + str(sign)] = value_dict
    client.close()

    return cached

inner_cached = get_data_from_mongo()


class StorageMongo(object):
    cached = inner_cached

    def __init__(self):
        self.client = MongoClient(HOST, PORT)
        self.collection = self.client[DB][COLLECTION]

    required_fields = ['s', 'p_code', 's_code', 'in_dt', 'out_dt', 'sign', 'cat', 'ct', 'stat']

    def validation(self, data):
        if not isinstance(data, dict):
            raise TypeError('Expected Dict object!')

        if 'stat' not in data:
            data.update(stat=2)

        if set(data.keys()) - set(self.required_fields):
            raise NameError('Expected keys: {}'.format(self.required_fields))

        if isinstance(data['sign'], basestring) and data['sign'] not in {'1', '0'}:
            raise ValueError('Key `sign` expected string and value is 0 or 1 !')

    def filter(self, p_code, s_code, **kwargs):
        ft = p_code + s_code
        cache = self.__class__.cached

        if ft not in cache:
            cache[ft] = {'ct': kwargs['ct']}
            return True, ft
        else:
            in_ft = ft + '1'
            if in_ft in cache:
                # 该指数曾被纳入， 又被剔除， 现需重新加入
                cache[in_ft] = {'ct': kwargs['ct']}
                return True, in_ft
        return False, ft

    def insert2mongo(self, docs_or_list):
        bulk = docs_or_list if isinstance(docs_or_list, (tuple, list)) else [docs_or_list]

        for each_docs in bulk:
            self.validation(each_docs)
            try:
                is_insert, uid = self.filter(**each_docs)

                if is_insert:
                    self.cached[uid]['_id'] = self.collection.insert(each_docs)
                else:
                    query = {'_id': self.cached[uid]['_id']}
                    docs = {'$set': {'ct': each_docs['ct']}}
                    self.collection.update(query, docs)
                    pass
            except Exception:
                pass

    def eliminate(self):
        format_dt = '%Y%m%d'
        td = datetime.now()
        required_docs = defaultdict(list)
        required_fields = {'p_code': 1, 's_code': 1, 'ct': 1, 'sign': 1}

        for r_docs in self.collection.find({}, required_fields):
            key = r_docs['p_code'] + r_docs['s_code']

            # 在未处理剔除之前的指数抓取都默认为是未剔除状态
            if r_docs['sign'] == '0':
                required_docs[key].append((r_docs['ct'], r_docs['_id']))

        for ps_key, values in required_docs.iteritems():
            values.sort(key=lambda item: itemgetter(item, 0))

        for key, values_list in required_docs:
            ct = values_list[0][0]
            cond = {'_id': values_list[0][1]}
            setdata = {'$set':
                {
                    'sign': '1',
                    'ct': datetime.now().strftime('%Y%m%d%H%M%S'),
                    'out_dt': (td - timedelta(days=1)).strftime(format_dt),
                }
            }

            if ct[:8] != td.strftime(format_dt):
                self.collection.update(cond, setdata)
        self.close()

    def close(self):
        self.client.close()


