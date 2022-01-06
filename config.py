# -*- coding: utf-8 -*-

from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.jobstores.redis import RedisJobStore

# 数据库配置信息
CONFIG = {
    # 数据库默认配置信息，必选，且索引必须为0
    0: {
        "host": '127.0.0.1',  # 可选，默认127.0.0.1
        "user": 'root',  # 可选，默认root
        'password': 'root',  # 必选
        'database': 'waipay02',  # 必选
        'port': '3306',  # 可选，默认3306
        'dbms': 'mysql',  # 可选，默认mysql
        'charset': 'utf8',  # 可选
        'DB_DEBUG': True,  # 可选，是否开启DEBUG模式，请在系统上线后关闭DEBUG模式
        'autocommit': True  # 开启自动提交事务
    },

    # 可选，数据库配置，'1'可以是任意字符串
    '1': {
        'database': 'db_name2',  # 必选
    },
}

REDIS = {
    'host': '127.0.0.1',
    'port': '6379',
    'db': 7,
    'password': ''
}

JOB_STORES = {
    'redis': RedisJobStore(**REDIS)
}

EXECUTORS = {
    'default': ThreadPoolExecutor(10),  # 默认线程数
    'processpool': ProcessPoolExecutor(3)  # 默认进程
}

OS_PATH = '/home/ec2-user/project_waipay/waipay_yin/waipay/'
