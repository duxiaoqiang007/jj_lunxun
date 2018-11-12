from tasks.db_connect import con_oracle, cursor
import time
from tasks.get_wechat import message_sub_dict
import datetime


import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
# 节点编号，节点名称，按提单/箱
# 按箱--0，按提单--1
message_queue = {
   "c10":{
       "node_id": 'c10',
       "node_name": '空箱出闸口',
       "mes_type": '0',
       "mes_content": '提空箱出场，拖车号：{}'
}, "c20": {
        "node_id": 'c20',
        "node_name": '出口重箱预录',
        "mes_type": '0',
        "mes_content": '出口重箱预录已完成'
}, "c30": {
        "node_id": 'c30',
        "node_name": '重箱落位',
        "mes_type": '0',
        "mes_content": '重箱已返回码头，场箱位：{}，中理闸口是否录入：是'
}, "c40": {
        "node_id": 'c40',
        "node_name": '运抵',
        "mes_type": '0',
        "mes_content": '运抵已收到正常回执'
}, "c50": {
        "node_id": 'c50',
        "node_name": '海关放行',
        "mes_type": '1',
        "mes_content": '海关已放行'
}, "c60": {
        "node_id": 'c60',
        "node_name": '船舶离港',
        "mes_type": '1',
        "mes_content": '船舶已离港，船名航次：{}'
}, "j10": {
        "node_id": 'j10',
        "node_name": '进口转关单已获取',
        "mes_type": '1',
        "mes_content": '进口转关单已获取'
}, "j20": {
        "node_id": 'j20',
        "node_name": '卸船完工',
        "mes_type": '1',
        "mes_content": '卸船作业完毕，转关单箱数：{}，本次卸船箱数：{}'
}, "j30": {
        "node_id": 'j30',
        "node_name": '进口核销完成',
        "mes_type": '1',
        "mes_content": '进口核销完毕'
}, "j40": {
        "node_id": 'j40',
        "node_name": '海关放行',
        "mes_type": '1',
        "mes_content": '已海关放行'
}, "j50": {
        "node_id": 'j50',
        "node_name": '提重计划',
        "mes_type": '1',
        "mes_content": '提重计划已受理, 计划号：{}'
}, "j60": {
        "node_id": 'j60',
        "node_name": '提箱出场（出闸）',
        "mes_type": '0',
        "mes_content": '已提箱出场'
}
}

oracle_con = con_oracle()
oracle_cursor = cursor(oracle_con)
message_sub = message_sub_dict()
dic_mes = {}


# rows ：SQL执行得到的结果
# id:哪个步骤的sql语句，对应message_queue
def insert_message_ctn(rows,id, billno, wechat_message_rows ,*args,**kwargs):
    dic_mes = {}
    local_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    if len(rows):
        # rows 数据库中查询出来的
        # wechat_message_rows是消息队列中查出来某个节点的插入的数据
        # message_sub 某提单号下的订阅人
        for row in rows:
            mes_content = row['MES_CONTENT'] if row['MES_CONTENT'] else ''
            mes_content = message_queue[id]['mes_content'].format(mes_content)
            dic_mes['BILLNO'] = row["BILLNO"]
            dic_mes['CTNNO'] = row["CTNNO"]
            for k, v in message_sub[row["BILLNO"]].items():
                dic_mes['OPEN_ID'] = v
                if dic_mes not in wechat_message_rows:
                    # 自增主键，订阅id,提单号，箱号，节点编号，节点名称，记录时间，消息内容，消息类型，是否推送
                    # insert_sql="INSERT INTO wechat_message(SUB_ID, BILLNO, CTNNO,NODE_ID,NODE_NAME,REC_TIME,MES_CONTENT,MES_TYPE,IF_PUSH) VALUES('"+str(k)+"', '"+billno+"','"+row['CTNNO']+"','"+message_queue[id]['node_id']+"','"+message_queue[id]['node_name']+"',to_date('"+local_time+"','yyyy-mm-dd hh24:mi:ss'),'"+message_queue[id]['mes_content'] + mes_content +"','"+message_queue[id]['mes_type']+"','0')"

                    insert_sql = "INSERT INTO wechat_message(SUB_ID, BILLNO, CTNNO,NODE_ID,NODE_NAME,REC_TIME,MES_CONTENT,MES_TYPE,IF_PUSH) VALUES('" + str(
                        k) + "', '" + billno + "','" + row['CTNNO'] + "','" + message_queue[id]['node_id'] + "','" + \
                                 message_queue[id][
                                     'node_name'] + "',to_date('" + local_time + "','yyyy-mm-dd hh24:mi:ss'),'" + \
                                  mes_content + "','" + message_queue[id][
                                     'mes_type'] + "','0')"
                    oracle_cursor.execute(insert_sql)
            oracle_con.commit()
            print('finish',id,billno)
    else:
        print('null',id)

def insert_message_billno(rows,id, billno, wechat_message_rows ,*args,**kwargs):
    dic_mes = {}
    local_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    if len(rows):
        # rows 数据库中查询出来的
        # wechat_message_rows是消息队列中查出来某个节点的插入的数据
        # message_sub 某提单号下的订阅人
        for row in rows:
            mes_content = row['MES_CONTENT'] if row['MES_CONTENT'] else ''
            mes_content = message_queue[id]['mes_content'].format(mes_content)
            dic_mes['BILLNO'] = row["BILLNO"]
            for k, v in message_sub[row["BILLNO"]].items():
                dic_mes['OPEN_ID'] = v
                if dic_mes not in wechat_message_rows:
                    # 自增主键，订阅id,提单号，箱号，节点编号，节点名称，记录时间，消息内容，消息类型，是否推送
                    # insert_sql="INSERT INTO wechat_message(SUB_ID, BILLNO, CTNNO,NODE_ID,NODE_NAME,REC_TIME,MES_CONTENT,MES_TYPE,IF_PUSH) VALUES('"+str(k)+"', '"+billno+"','"+row['CTNNO']+"','"+message_queue[id]['node_id']+"','"+message_queue[id]['node_name']+"',to_date('"+local_time+"','yyyy-mm-dd hh24:mi:ss'),'"+message_queue[id]['mes_content'] + mes_content +"','"+message_queue[id]['mes_type']+"','0')"

                    insert_sql = "INSERT INTO wechat_message(SUB_ID, BILLNO,NODE_ID,NODE_NAME,REC_TIME,MES_CONTENT,MES_TYPE,IF_PUSH) VALUES('" + str(
                        k) + "', '" + billno + "','" + message_queue[id]['node_id'] + "','" + \
                                 message_queue[id][
                                     'node_name'] + "',to_date('" + local_time + "','yyyy-mm-dd hh24:mi:ss'),'" + \
                                  mes_content + "','" + message_queue[id][
                                     'mes_type'] + "','0')"
                    oracle_cursor.execute(insert_sql)
            oracle_con.commit()
            print('finish',id,billno)
    else:
        print('null',id)

