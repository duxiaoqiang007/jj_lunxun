
from kombu import Exchange, Queue
#broke设置redis。
BROKER_URL = 'redis://localhost:6379/0'

#使用redis存储任务状态和结果。
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'

# #设置轮询时间
# CELERYBEAT_SCHEDULE = {
#     "15-min": {
#         'task': 'tasks.add',
#         'schedule': crontab(minute='*/15')
#     }
# }

CELERY_QUEUES = (
    Queue("default", Exchange("default"), routing_key="default"),
    Queue("ctn_in", Exchange("ctn_in"), routing_key="ctn_in"),
    Queue("ctn_out", Exchange("ctn_out"), routing_key="ctn_out"),
)
CELERY_ROUTES = {
    'tasks.ctn_in': {"queue": "for_in", "routing_key": "ctn_in"},
    'tasks.ctn_out': {"queue": "for_out", "routing_key": "ctn_in"},
}


ORACLE_LJ="shdsj/shdsj@192.168.96.3:1521/sjpc"
# server    数据库服务器名称或IP
# user      用户名
# password  密码
# database  数据库名称
SQL_LJ = {'server': '192.168.96.10', 'user': 'wechat', 'password': 'wechat2016', 'database': 'pbhan'}


