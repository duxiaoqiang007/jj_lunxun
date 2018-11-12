import celeryconfig
from celery import Celery
import node_tasks
from get_wechat import message_sub_dict
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

broker = celeryconfig.BROKER_URL
app = Celery('tasks',  broker=broker)


@app.on_after_configure.connect
def setup_periodic_tasks(sender,**kwargs):
    sender.add_periodic_task(900.0, start, name='start')


@app.task()
def start():
    # 获取open_id 和 订阅的提单号字典
    mes_sub_dict = message_sub_dict()
    # 订单号应该由订阅表中读取
    for v in mes_sub_dict:
        # node_tasks.kxczk(v)
        # node_tasks.ckzxyl(v)
        # node_tasks.zxlw(v)
        # node_tasks.yd(v)
        # node_tasks.hgfx_out(v)
        node_tasks.cblg(v)
        # node_tasks.jkzgd(v)
        # # 没有找到合适的数据
        # node_tasks.xcwg(v)
        # node_tasks.jkhx(v)
        # node_tasks.hgfx_in(v)
        # node_tasks.tzjh(v)
        # node_tasks.txcc(v)
start()


