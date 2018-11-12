
from kombu import Exchange, Queue
#broke设置redis。
BROKER_URL = 'redis://localhost:6379/0'

#使用redis存储任务状态和结果。
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'

#设置轮询时间
CELERYBEAT_SCHEDULE = {
    "ctn_in": {
        'task': 'celery_start.start_in',
        'schedule': 900.0,
        'options':{'queue':'ctn_in','routing_key':'ctn_in'}
    },
    "ctn_out": {
        'task': 'celery_start.start_out',
        'schedule': 900.0,
        'options':{'queue':'ctn_out','routing_key':'ctn_out'}

    }
}

CELERY_QUEUES = (
    Queue("default", Exchange("default"), routing_key="default"),
    Queue("ctn_in", Exchange("ctn_in"), routing_key="ctn_in"),
    Queue("ctn_out", Exchange("ctn_out"), routing_key="ctn_out"),
)
CELERY_ROUTES = {
    'celery_start.start_in': {"queue": "ctn_in", "routing_key": "ctn_in"},
    'celery_start.start_out': {"queue": "ctn_out", "routing_key": "ctn_out"},
}





