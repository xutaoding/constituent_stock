# -*- coding: utf-8 -*-
import os
import time
from datetime import datetime
from os.path import dirname, abspath

from pymongo import MongoClient
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor

from crawler import *
from crawler.csindex.csindex import run_path
from conf import logger
from conf.receiver import receiver
from conf import CAN_HOST, CAN_PORT, CAN_DB, CAN_COLLECTION
from utils.mail import Sender


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
    # 'default': ThreadPoolExecutor(4),
    'default': ProcessPoolExecutor(4),
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
    query = {'trad': 1}

    calendars = {docs['dt'] for docs in collection.find(query, {'dt': 1}) if docs['dt'].startswith(today)}
    client.close()
    return calendars


def crawl_sse_index():
    try:
        SSEIndex().crawl()  # 上海交易所指数
    except Exception as e:
        logger.info('SSE crawl error: type <{typ}>, msg <{msg}>\n'.format(typ=e.__class__, msg=e))


def crawl_szse_index():
    try:
        SzseIndex().crawl()  # 深圳交易所指数
    except Exception as e:
        logger.info('SZSE crawl error: type <{typ}>, msg <{msg}>\n'.format(typ=e.__class__, msg=e))


def crawl_cn_index():
    try:
        CNIndex().main()  # CNINDEX 网站指数
    except Exception as e:
        logger.info('CNindex crawl error: type <{typ}>, msg <{msg}>\n'.format(typ=e.__class__, msg=e))


def crawl_cs_index():
    from scrapy.crawler import CrawlerProcess

    try:
        # 中证指数网站
        # cp = CrawlerProcess()
        # cp.crawl(CsindexSpider())
        # cp.start()
        if run_path.endswith('.py'):
            os.system('python %s' % run_path)
        elif run_path.endswith('.pyc'):
            os.system('python %s' % run_path[:-1])
    except Exception as e:
        logger.info('CSIndex crawl error: type <{typ}>, msg <{msg}>\n'.format(typ=e.__class__, msg=e))


def spider_indexes():
    today = datetime.now().strftime('%Y-%m-%d')

    if today not in is_workday():
        return

    sse_start = time.time()
    crawl_sse_index()
    logger.info('SSE site crawl Done!\n')

    szse_start = time.time()
    crawl_szse_index()
    logger.info('SZSE site crawl Done!\n')

    cn_start = time.time()
    crawl_cn_index()
    logger.info('CNINDES site crawl Done!\n')

    cs_start = time.time()
    crawl_cs_index()
    logger.info('CSINDEX site crawl Done!\n')
    end = time.time()

    subject = u'指数成分股抓取完成'
    text = u'上交所网站抓取时间: %s\n' % (szse_start - sse_start) + \
           u'深交所网站抓取时间: %s\n' % (cn_start - szse_start) + \
           u'CNINDEX 网站抓取时间: %s\n' % (cs_start - cn_start) + \
           u'CSINDEX 网站抓取时间: %s\n' % (end - cs_start)
    Sender(receivers=receiver).send_email(subject, text)

# app.add_job(crawl_sse_index, trigger='cron', **trigger_kwargs['sse'])
# app.add_job(crawl_szse_index, trigger='cron', **trigger_kwargs['szse'])
# app.add_job(crawl_cn_index, trigger='cron', **trigger_kwargs['cnindex'])
# app.add_job(crawl_cs_index, trigger='cron', **trigger_kwargs['csindex'])

app.add_job(spider_indexes, trigger='cron', hour='9', minute='20')
# app.add_job(spider_indexes, trigger='cron', hour='13', minute='0')
app.start()

