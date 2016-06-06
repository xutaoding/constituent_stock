import os
from os.path import dirname, abspath

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

from crawler import *
from utils.util import StorageMongo


def create_sqlite():
    sqlite_path = dirname(abspath(__file__))
    for sql_path in os.listdir(sqlite_path):
        if sql_path.endswith('.db'):
            os.remove(os.path.join(sqlite_path, sql_path))

create_sqlite()


def crawl_sse_index():
    SSEIndex().crawl()


def crawl_szse_index():
    SZSEIndex().upload()


def crawl_cnindex():
    CNIndex().main()


def crawl_csindex():
    from scrapy.crawler import CrawlerProcess
    cp = CrawlerProcess()
    cp.crawl(CsindexSpider())
    cp.start()

jobstores = {
    'default': SQLAlchemyJobStore(url='sqlite:///jobs.db')
}

# using ThreadPoolExecutor as default other than ProcessPoolExecutor(not work) to executors
executors = {
    'default': ThreadPoolExecutor(20),
}

job_defaults = {
    'coalesce': False,
    'max_instances': 1
}
app = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)


@app.scheduled_job(trigger='corn', hour='23')
def eliminate():
    StorageMongo().eliminate()

trigger = 'cron'
trigger_kwargs = {'hour': '9'}

app.add_job(crawl_sse_index, trigger=trigger, **trigger_kwargs)
app.add_job(crawl_szse_index, trigger=trigger, **trigger_kwargs)
app.add_job(crawl_cnindex, trigger=trigger, **trigger_kwargs)
app.add_job(crawl_csindex, trigger=trigger, **trigger_kwargs)

app.start()

