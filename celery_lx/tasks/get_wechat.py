from setting.sqlconfig import  sql_wechat_message,sql_wechat_sub,sql_wechat_sub_only_billno
from tasks.db_connect import con_oracle,cursor
conn_oracle = con_oracle()
oracle_cursor = conn_oracle.cursor(conn_oracle)


# 把list转为字典
def dict_fetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


# 获取当前wechat_message中相关信息 ,后续用来判断轮询查到的数据是否已经插入
def get_message_queue(node_id,billno):
    sql_wechat_mes = sql_wechat_message(node_id,billno)
    oracle_cursor.execute(sql_wechat_mes)
    message_queue = dict_fetchall(oracle_cursor)
    for v in message_queue:
        if v["CTNNO"] is None:
            del v["CTNNO"]
    return message_queue


# 获取所有的提单号
sql_wechat_billno = sql_wechat_sub_only_billno()
oracle_cursor.execute(sql_wechat_billno)
wechat_billno = oracle_cursor.fetchall()

sql_wechat_subscribe = sql_wechat_sub()
oracle_cursor.execute(sql_wechat_subscribe)
wechat_sub = dict_fetchall(oracle_cursor)

# 转为字典格式
def message_sub_dict():
    wechat_billno_in = get_wechat_billno()
    wechat_sub_in = get_wechat_sub()
    wechat_messsage_sub ={}
    for v in wechat_billno_in:
        sub = {}
        for value in wechat_sub_in:
            if value['BILLNO'] == v[0]:
                sub[value['ID']] = value['OPEN_ID']
        wechat_messsage_sub[v[0]] = sub
    return wechat_messsage_sub


# 获取订阅人以及订阅提单号
def get_wechat_sub():
    global wechat_sub
    if wechat_sub:
        return wechat_sub
    else:
        sql_wechat_subscribe = sql_wechat_sub()
        oracle_cursor.execute(sql_wechat_subscribe)
        wechat_sub = dict_fetchall(oracle_cursor)
        return wechat_sub


# 获取订阅的提单号
def get_wechat_billno():
    global wechat_billno
    if wechat_billno:
        return wechat_billno
    else:
        sql_wechat_billno = sql_wechat_sub_only_billno()
        oracle_cursor.execute(sql_wechat_billno)
        wechat_billno = oracle_cursor.fetchall()
        return wechat_billno