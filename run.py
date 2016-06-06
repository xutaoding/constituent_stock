from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

from crawler import *
from utils.util import StorageMongo

jobstores = {
    'default': MemoryJobStore()
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


@app.scheduled_job(trigger='corn')
def eliminate():
    StorageMongo().eliminate()

trigger = 'interval'
trigger_kwargs = {}

app.add_job(crawl_sse_index, trigger=trigger, trigger_args=trigger_kwargs)
app.add_job(crawl_szse_index, trigger=trigger, trigger_args=trigger_kwargs)
app.add_job(crawl_cnindex, trigger=trigger, trigger_args=trigger_kwargs)
app.add_job(crawl_csindex, trigger=trigger, trigger_args=trigger_kwargs)

app.start()

