from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

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
    pass


def crawl_szse_index():
    pass


def crawl_cnindex():
    pass


def crawl_scindex():
    pass

trigger = 'interval'
trigger_kwargs = {}

app.add_job(crawl_sse_index, trigger=trigger, trigger_args=trigger_kwargs)
app.add_job(crawl_szse_index, trigger=trigger, trigger_args=trigger_kwargs)
app.add_job(crawl_cnindex, trigger=trigger, trigger_args=trigger_kwargs)
app.add_job(crawl_scindex, trigger=trigger, trigger_args=trigger_kwargs)

app.start()
