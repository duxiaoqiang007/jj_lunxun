from setting import celeryconfig
from celery import Celery
from tasks import node_tasks
from tasks.get_wechat import message_sub_dict
from tasks.db_connect import con_oracle,con_sql,cursor
from celery import Task
from tasks.wechat_java_api import get_wechat_java
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

app = Celery()

app.config_from_object(celeryconfig) # 获取配置
# @app.on_after_configure.connect
# def setup_periodic_tasks(sender,**kwargs):
#     sender.add_periodic_task(900.0, start_out, name='start_out')
#     sender.add_periodic_task(900.0, start_in, name='start_out')

# 获取数据库连接
oracle_con = con_oracle()
oracle_cursor = cursor(oracle_con)
sql_con = con_sql()
sql_cursor = cursor(sql_con)


class CloseCon(Task):
    _if_stop = [0, 0]

    def on_succrss(self,retval,task_id,*args,**kwargs):
        if self.if_stop == [1, 1]:
            oracle_cursor.close()
            oracle_con.close()
            sql_cursor.close()
            sql_con.close()
            print('con close!')
#             调用java后台模板消息接口。
            get_wechat_java()


@app.task(base = CloseCon)
def start_out():
    # 获取open_id 和 订阅的提单号字典
    mes_sub_dict = message_sub_dict()
    # # 开始运行时，把成功的参数回调为0
    start_out._if_stop[0] = 0
    # 订单号应该由订阅表中读取
    for v in mes_sub_dict:
        node_tasks.kxczk(v)
        node_tasks.ckzxyl(v)
        node_tasks.zxlw(v)
        node_tasks.yd(v)
        node_tasks.hgfx_out(v)
        node_tasks.cblg(v)
    start_out._if_stop[0] = 1


@app.task(base = CloseCon)
def start_in():
    # 获取open_id 和 订阅的提单号字典
    mes_sub_dict = message_sub_dict()
    # # 开始运行时，把成功的参数回调为0
    start_in._if_stop[1] = 0
    # 订单号应该由订阅表中读取
    for v in mes_sub_dict:
        node_tasks.jkzgd(v)
        # 没有找到合适的数据
        node_tasks.xcwg(v)
        node_tasks.jkhx(v)
        node_tasks.hgfx_in(v)
        node_tasks.tzjh(v)
        node_tasks.txcc(v)
    start_in._if_stop[1] = 1

# start_in()
# start_out()
get_wechat_java()