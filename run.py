# -*- coding: utf-8 -*-
import os
from datetime import datetime
from os.path import dirname, abspath

from pymongo import MongoClient
from scrapy.crawler import CrawlerProcess
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

from crawler import *
from conf import logger
from conf import CAN_HOST, CAN_PORT, CAN_DB, CAN_COLLECTION


def create_sqlite():
    sqlite_path = dirname(abspath(__file__))
    for sql_path in os.listdir(sqlite_path):
        if sql_path.endswith('.db'):
            os.remove(os.path.join(sqlite_path, sql_path))

create_sqlite()


jobstores = {
    'default': SQLAlchemyJobStore(url='sqlite:///jobs.db')
}

# using ThreadPoolExecutor as default other than ProcessPoolExecutor(not work) to executors
executors = {
    'default': ThreadPoolExecutor(4),
}

job_defaults = {
    'coalesce': False,
    'max_instances': 1
}

trigger_kwargs = {
    'sse': {'hour': '9', 'minute': '20'},
    'szse': {'hour': '9', 'minute': '22'},
    'csindex': {'hour': '9', 'minute': '26'},
    'cnindex': {'hour': '9', 'minute': '28'},
}

app = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)


def is_workday():
    today = datetime.now().strftime('%Y')
    client = MongoClient(CAN_HOST, CAN_PORT)
    collection = client[CAN_DB][CAN_COLLECTION]

    calendars = {docs['dt'] for docs in collection.find({}, {'dt': 1}) if docs['dt'].startswith(today)}
    client.close()
    return calendars


def crawl_sse_index():
    today = datetime.now().strftime('%Y-%m-%d')

    try:
        if today not in is_workday():
            return

        SSEIndex().crawl()  # 上海交易所指数
    except Exception as e:
        logger.info('SSE crawl error: type <{typ}>, msg <{msg}>'.format(typ=e.__class__, msg=e))


def crawl_szse_index():
    today = datetime.now().strftime('%Y-%m-%d')

    try:
        if today not in is_workday():
            return

        SZSEIndex().upload()  # 深圳交易所指数
    except Exception as e:
        logger.info('SZSE crawl error: type <{typ}>, msg <{msg}>'.format(typ=e.__class__, msg=e))


def crawl_cn_index():
    today = datetime.now().strftime('%Y-%m-%d')

    try:
        if today not in is_workday():
            return

        CNIndex().main()  # CNINDEX 网站指数
    except Exception as e:
        logger.info('CNindex crawl error: type <{typ}>, msg <{msg}>'.format(typ=e.__class__, msg=e))


def crawl_cs_index():
    today = datetime.now().strftime('%Y-%m-%d')

    try:
        if today not in is_workday():
            return

        # 中证指数网站
        cp = CrawlerProcess()
        cp.crawl(CsindexSpider())
        cp.start()
    except Exception as e:
        logger.info('SSIndex crawl error: type <{typ}>, msg <{msg}>'.format(typ=e.__class__, msg=e))


def spider_indexes():
    today = datetime.now().strftime('%Y-%m-%d')

    if today not in is_workday():
        return

    crawl_sse_index()
    crawl_szse_index()
    crawl_cn_index()
    crawl_cs_index()

# app.add_job(crawl_sse_index, trigger='cron', **trigger_kwargs['sse'])
# app.add_job(crawl_szse_index, trigger='cron', **trigger_kwargs['szse'])
# app.add_job(crawl_cn_index, trigger='cron', **trigger_kwargs['cnindex'])
# app.add_job(crawl_cs_index, trigger='cron', **trigger_kwargs['csindex'])

app.add_job(spider_indexes, trigger='cron', hour='11', minute='50')
app.start()

