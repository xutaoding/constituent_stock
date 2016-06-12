# -*- coding: utf-8 -*-
import os
from os.path import dirname, abspath

from scrapy.crawler import CrawlerProcess
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

from crawler import *
from conf import logger
from utils.util import StorageMongo


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
app = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)


@app.scheduled_job(trigger='cron', hour='17')
def crawl_index_jobs():
    try:
        SSEIndex().crawl()  # 上海交易所指数
    except Exception as e:
        logger.info('SSE crawl error: type <{typ}>, msg <{msg}>'.format(typ=e.__class__, msg=e))

    try:
        SZSEIndex().upload()  # 深圳交易所指数
    except Exception as e:
        logger.info('SZSE crawl error: type <{typ}>, msg <{msg}>'.format(typ=e.__class__, msg=e))

    try:
        CNIndex().main()  # CNINDEX 网站指数
    except Exception as e:
        logger.info('CNindex crawl error: type <{typ}>, msg <{msg}>'.format(typ=e.__class__, msg=e))

    try:
        # 中证指数网站
        cp = CrawlerProcess()
        cp.crawl(CsindexSpider())
        cp.start()
    except Exception as e:
        logger.info('SSIndex crawl error: type <{typ}>, msg <{msg}>'.format(typ=e.__class__, msg=e))


@app.scheduled_job(trigger='corn', hour='23')
def eliminate():
    try:
        StorageMongo().eliminate()
    except Exception as e:
        logger.info('Eliminate index error: type <{typ}>, msg <{msg}>'.format(typ=e.__class__, msg=e))

app.start()

