# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import hashlib
from datetime import datetime
from operator import itemgetter
from collections import defaultdict

from pymongo import MongoClient
from conf import logger
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


class StorageMongo(object):
    default_indexes = ['ct', '_id']

    def __init__(self):
        self.client = MongoClient(HOST, PORT)
        self.collection = self.client[DB][COLLECTION]

        self.created_index()
        self.need_index = set()
        self.cached = self.get_data_from_mongo()

    required_fields = ['s', 'p_code', 's_code', 'in_dt', 'out_dt', 'sign', 'cat', 'ct', 'stat']

    def get_data_from_mongo(self, unset=True):
        cached = set()
        required_docs = self.get_ordered_items()

        if unset is False:
            setattr(self, 'latest_indexes', required_docs)

        for _k, _v in required_docs.iteritems():
            sign = _v[0][-1]
            key = _k + sign if sign == '1' else _k
            cached.add(key)
        return cached

    def get_ordered_items(self):
        required_docs = defaultdict(list)
        required_fields = {'p_code': 1, 's_code': 1, 'ct': 1, 'sign': 1}

        for r_docs in self.collection.find({}, required_fields).sort([('ct', -1)]):
            key = r_docs['p_code'] + r_docs['s_code']

            # 在未处理剔除之前的指数抓取都默认为是未剔除状态
            required_docs[key].append((r_docs['ct'], r_docs['_id'], r_docs['sign']))

        for ps_key, values in required_docs.iteritems():
            values.sort(key=lambda item: itemgetter(item, 0))
        return required_docs

    def list_indexes(self):
        indexes = []

        for key, value in self.collection.index_information().iteritems():
            try:
                index = value['key'][0][0]
                indexes.append(index)
            except (KeyError, IndexError):
                pass
        return indexes

    def created_index(self):
        try:
            actual_indexes = self.list_indexes()

            for index in self.default_indexes:
                if index not in actual_indexes:
                    self.collection.create_index(index)
        except:
            pass

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
        cache = self.cached
        self.need_index.add(ft)

        if ft not in cache:
            cache.add(ft)
            return True
        else:
            in_ft = ft + '1'
            if in_ft in cache:
                # 该指数曾被纳入， 又被剔除， 现需重新加入
                return True
        return False

    def insert2mongo(self, docs_or_list):
        bulk = docs_or_list if isinstance(docs_or_list, (tuple, list)) else [docs_or_list]

        for each_docs in bulk:
            self.validation(each_docs)
            is_insert = self.filter(**each_docs)
            try:
                if is_insert:
                    self.collection.insert(each_docs)
            except Exception:
                pass

    def eliminate(self):
        need_indexes = self.need_index
        mongo_indexes = self.get_data_from_mongo(unset=True)

        diff_set = mongo_indexes - need_indexes

        try:
            latest_docs = self.latest_indexes

            for ps_key in diff_set:
                _id = latest_docs[ps_key][0][1]
                query = {'_id': _id}
                setdata = {'$set': {'sign': '1', 'ct': datetime.now().strftime('%Y%m%d%H%M%S')}}
                self.collection.update(query, setdata)
        except (AttributeError, KeyError, IndexError) as e:
            logger.info('`Eliminate` crawl error: type <{typ}>, msg <{msg}>'.format(typ=e.__class__, msg=e))

    def close(self):
        self.client.close()


