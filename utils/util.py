# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import re
import hashlib
from datetime import datetime, date, timedelta
from collections import defaultdict

from pymongo import MongoClient
from conf import logger
from mail import Sender
from conf import HOST, PORT, DB, COLLECTION
from conf.receiver import receiver
from conf.indexes import REQUIRED_INDEXES as _RINDEX


def get_md5(value):
    if not isinstance(value, basestring):
        raise ValueError('md5 must string!')
    m = hashlib.md5()
    try:
        m.update(value)
    except UnicodeEncodeError:
        m.update(value.encode('u8'))
    return m.hexdigest()


class BaseMongo(object):
    def __init__(self):
        self.client = MongoClient(HOST, PORT)
        self.collection = self.client[DB][COLLECTION]


class StorageMongo(BaseMongo):
    default_indexes = ['cat']

    def __init__(self, category):
        super(StorageMongo, self).__init__()

        self.created_index()
        self.need_index = set()
        self.using_category = category
        self.insert_mongo_data = []
        self.cached = self.get_data_from_mongo()

    required_fields = ['p_abbr', 'p_code', 's_code', 'in_dt', 'out_dt', 'sign', 'cat', 'crt', 'upt',  'stat']

    def get_data_from_mongo(self, unset=True, including_sign=True, query=True):
        cached = set()
        required_docs = self.get_ordered_items(query)

        if unset is False:
            setattr(self, 'latest_indexes', required_docs)

        for _k, _v in required_docs.iteritems():
            key = _k
            out_dt = _v[0]['out_dt']
            cached.add(key)

            if including_sign and out_dt:
                cached.add(key + '1')
        return cached

    def get_ordered_items(self, query=False):
        """
        Just obtain docs data from mongo, which crawled with web
        :param query: query condition from mongo
        :return: dict of list, like: [{...}, ...]
        """
        required_docs = defaultdict(list)

        if not query:
            query_cond = {'cat': re.compile(r'%s' % self.using_category)}
        else:
            query_cond = {}

        for r_docs in self.collection.find(query_cond):
            key = r_docs['p_code'] + r_docs['s_code']

            # 在未处理剔除之前的指数抓取都默认为是未剔除状态
            required_docs[key].append(r_docs)

        for ps_key, values in required_docs.iteritems():
            values.sort(key=lambda item: item['in_dt'], reverse=True)
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

    def validation(self, data, rewrite_in_dt=False):
        if not isinstance(data, dict):
            raise TypeError('Expected Dict object!')

        if 'stat' not in data:
            data.update(stat=2)

        if set(data.keys()) - set(self.required_fields):
            raise NameError('Expected keys: {}'.format(self.required_fields))

        if isinstance(data['sign'], basestring) and data['sign'] not in {'1', '0'}:
            raise ValueError('Key `sign` expected string and value is 0 or 1 !')

        if rewrite_in_dt:
            data['in_dt'] = date.today().strftime('%Y%m%d')

        s_code = data['s_code']
        if len(s_code) != 6 and s_code.isdigit():
            data['s_code'] = '0' * (6 - len(s_code)) + s_code

    def filter(self, p_code, s_code, **kwargs):
        ft = p_code + s_code
        cache = self.cached
        self.need_index.add(ft)

        if p_code not in _RINDEX:
            return False

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
            self.validation(each_docs, rewrite_in_dt=True)
            is_insert = self.filter(**each_docs)
            try:
                if is_insert:
                    p_s_code = each_docs['p_code'] + each_docs['s_code']
                    self.collection.insert(each_docs)
                    self.insert_mongo_data.append(p_s_code)
            except Exception:
                pass

    def eliminate(self):
        must_remove_indexes = []
        need_indexes = self.need_index
        mongo_indexes = self.get_data_from_mongo(unset=False, including_sign=False, query=False)
        latest_docs = getattr(self, 'latest_indexes')

        diff_set = mongo_indexes - need_indexes

        # If have `diff_set`, group by `p_code`, then send email
        normal_indexes, no_normal_indexes = self.think_indexes_issue(diff_set)
        print 'set(mong0) - set(web)=', len(diff_set)
        print 'Normal:', len(normal_indexes), 'No Normal:', len(no_normal_indexes)

        for ps_key in normal_indexes:
            if ps_key in latest_docs:
                try:
                    _id = latest_docs[ps_key][0]['_id']
                    out_dt = latest_docs[ps_key][0]['out_dt']

                    if not out_dt:
                        query = {'_id': _id}
                        setdata = {
                            '$set': {'sign': '1', 'stat': 2, 'upt': datetime.now(),
                                     'out_dt': date.today().strftime("%Y%m%d")}
                        }
                        self.collection.update(query, setdata)
                        must_remove_indexes.append(ps_key)
                except (AttributeError, IndexError, KeyError) as e:
                    logger.info('`Eliminate` crawl error: type <{typ}>, msg <{msg}>'.format(typ=e.__class__, msg=e))

        if self.insert_mongo_data:
            subject = '%s 网站: 网页新增， 插入数据库的记录(新增)，望检查' % self.using_category.upper()
            self.send_email(subject, self.insert_mongo_data, prefix='web')

        if no_normal_indexes:
            subject = '%s 网站: 可能有问题的指数成分股(未剔除)， 请检查' % self.using_category.upper()
            self.send_email(subject, no_normal_indexes)

        if must_remove_indexes:
            subject = '%s 网站: 没有问题的指数成分股(已剔除)， 望检查' % self.using_category.upper()
            self.send_email(subject, must_remove_indexes)

    def think_indexes_issue(self, diff_indexes):
        normal_indexes = []
        no_normal_indexes = []
        issue_indexes_dict = defaultdict(list)

        for p_s_code in diff_indexes:
            key = p_s_code[:6]
            issue_indexes_dict[key].append(p_s_code)

        for _key, _value in issue_indexes_dict.iteritems():
            feimu_count = self.collection.find({'p_code': _key, 'cat': self.using_category}).count()

            if feimu_count and (len(_value) * 1.0 / feimu_count) >= 0.5:
                no_normal_indexes.extend(_value)
            else:
                normal_indexes.extend(_value)
        return normal_indexes, no_normal_indexes

    @staticmethod
    def send_email(subject, dataset, prefix='mongo'):
        temp_dict = defaultdict(list)

        for p_s_code in dataset:
            key = p_s_code[:6]
            temp_dict[key].append(p_s_code[6:])

        alone_attaches = [
            {
                'attach_name': prefix + '_' + k + '.txt',
                'attach_text': '\n'.join(v)
             }
            for k, v in temp_dict.iteritems()
            ]
        Sender(receivers=receiver).send_email(subject, subject, attaches=alone_attaches)

    def close(self):
        self.client.close()


class IndexFiltering(BaseMongo):
    def __init__(self):
        super(IndexFiltering, self).__init__()

        self.web_indexes = self._web_indexes()

    def _web_indexes(self):
        web_indexes = defaultdict(set)
        query = {'cat': re.compile(r'sse|szse|cnindex|csindex')}

        for docs in self.collection.find(query):
            web_indexes[docs['cat']].add(docs['p_code'] + docs['s_code'])
        self.client.close()
        return web_indexes

    def exclude_index(self, p_code, s_code, default_cat=None):
        need_cat = default_cat or ['sse', 'szse']
        for cat in need_cat:
            required_key = p_code + s_code
            if required_key in self.web_indexes[cat]:
                return False
        return True
