# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import re
import hashlib
from datetime import date

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
    cached = set()
    client = MongoClient(HOST, PORT)
    collection = client[DB][COLLECTION]

    fields = ['p_code', 's_code', 'sign']
    cursor = collection.find({}, {k: 1 for k in fields})

    for docs in cursor:
        sign = int(docs['sign'])
        ps_code = docs['p_code'] + docs['s_code']
        cached.add(ps_code)

        if sign:
            cached.add(ps_code + str(sign))
    client.close()

    return cached

inner_cached = get_data_from_mongo()


class StorageMongo(object):
    cached = inner_cached

    def __init__(self):
        self.client = MongoClient(HOST, PORT)
        self.collection = self.client[DB][COLLECTION]

    required_fields = ['s', 'p_code', 's_code', 'in_dt', 'out_dt', 'sign', 'cat', 'ct']

    def validation(self, data):
        if isinstance(data, dict):
            raise TypeError('Expected Dict object!')

        if set(data.keys()) - set(self.required_fields):
            raise NameError('Expected keys: {}'.format(self.required_fields))

        if isinstance(data['sign'], basestring) and data['sign'] in {1, 0}:
            raise ValueError('Key `sign` expected string and value is 0 or 1 !')

    def filter(self, p_code, s_code, **kwargs):
        ft = p_code + s_code
        cache = self.__class__.cached

        if ft not in cache:
            cache.add(ft)
            return True
        else:
            if ft + '1' in cache:
                # 该指数曾被纳入， 又被剔除， 现需重新加入
                return True
        return False

    def insert2mongo(self, docs_or_list):
        bulk = docs_or_list if isinstance(docs_or_list, (tuple, list)) else [docs_or_list]

        for each_docs in bulk:
            self.validation(each_docs)
            try:
                if self.filter(**each_docs):
                    self.collection.insert(each_docs)
            except Exception:
                pass

    def eliminate(self):
        today = str(date.today()).replace('-', '')
        pass

    def close(self):
        self.client.close()



