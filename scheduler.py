from apscheduler.schedulers.blocking import BlockingScheduler
from csv_import_mysql import *

from config import *

from import_result import import_result

from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED

import logging

logger = logging.getLogger('job')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='scheduler_log.txt',
                    filemode='a')


def job_listener(event):
    job = scheduler.get_job(event.job_id)
    if not event.exception:
        print('任务正常运行！')
        logger.info("jobname=%s|jobtrigger=%s|jobtime=%s|retval=%s", job.name, job.trigger,
                    event.scheduled_run_time, event.retval)

    else:
        print("任务出错了！！！！！")
        print(event.exception)
        logger.error("jobname=%s|jobtrigger=%s|errcode=%s|exception=[%s]|traceback=[%s]|scheduled_time=%s", job.name,
                     job.trigger, event.code,
                     event.exception, event.traceback, event.scheduled_run_time)


if __name__ == "__main__":
    scheduler = BlockingScheduler(jobstores=JOB_STORES, executors=EXECUTORS)
    # seconds = 40
    # minutes= 8
    print(datetime.datetime.now())
    scheduler.add_job(func=get_csv, trigger='interval', jobstore='redis', minutes=30,
                      start_date='2021-12-27 16:03:00')

    scheduler.add_job(func=post_order, trigger='interval', minutes=20, start_date='2021-12-27 16:03:00')

    scheduler.add_job(func=check_order_status, trigger='interval', minutes=40, start_date='2021-12-27 16:03:00')

    scheduler.add_job(func=import_result, trigger='cron', hour='18')

    scheduler.add_listener(job_listener, EVENT_JOB_ERROR | EVENT_JOB_MISSED | EVENT_JOB_EXECUTED)

    scheduler._logger = logging

    scheduler.start()

# scheduler.add_job(job, 'interval', minutes=1, seconds = 30, start_date='2019-08-29 22:15:00', end_date='2019-08-29 22:17:00', args=['job2'])
# 在每天 8 点，运行一次 job 方法
#   scheduler.add_job(job, 'cron', hour='8', args=['job2'])
