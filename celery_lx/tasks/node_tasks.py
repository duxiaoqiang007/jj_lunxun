from setting.sqlconfig import sql_mes,sql_wechat_sub
from tasks.db_connect import con_oracle,con_sql,cursor
from tasks.insert_message import insert_message_ctn,insert_message_billno
from tasks.get_wechat import get_message_queue
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


# 把list转为字典
def dict_fetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


# 获取数据库连接
oracle_con = con_oracle()
oracle_cursor = cursor(oracle_con)
sql_con = con_sql()
sql_cursor = cursor(sql_con)

# 空箱出闸口
def kxczk(billno):
    # 获取数据库字符串
    sql = sql_mes(billno,'')
    str_kxczk = sql.sql_kxczk()
    oracle_cursor.execute(str_kxczk)
    rows = dict_fetchall(oracle_cursor)
    if len(rows):
        wechat_message_rows = get_message_queue('c10',billno)
        # 如果取到值，则向消息队列中插入值
        insert_message_ctn(rows, "c10", billno, wechat_message_rows)


# 出口重箱预录
def ckzxyl(billno):
    sql = sql_mes(billno,'')
    str_ckzxyl = sql.sql_ckzxyl()
    oracle_cursor.execute(str_ckzxyl)
    rows = dict_fetchall(oracle_cursor)
    if len(rows):
        wechat_message_rows = get_message_queue('c20',billno)
        # 如果取到值，则向消息队列中插入值
        insert_message_ctn(rows, "c20", billno, wechat_message_rows)


# 重箱落位
def zxlw(billno):
    sql = sql_mes(billno,'')
    str_zxlw_tos = sql.sql_zxlw_tos()

    oracle_cursor.execute(str_zxlw_tos)
    rows_tos = dict_fetchall(oracle_cursor)
    # 可以优化，使用sql中in方法
    for v in rows_tos:
        sql.ctnno = v['CTNNO']
        str_zxlw_zl = sql.sql_zxlw_zl()
        sql_cursor.execute(str_zxlw_zl)
        row_zl = sql_cursor.fetchall()
        if len(row_zl) == 0:
            rows_tos.remove(v)
    if len(rows_tos):
        wechat_message_rows = get_message_queue('c30',billno)
        # 如果取到值，则向消息队列中插入值
        insert_message_ctn(rows_tos, "c30", billno, wechat_message_rows)


# 运抵
def yd(billno):
    sql = sql_mes(billno, '')
    str_yd = sql.sql_yd()
    sql_cursor.execute(str_yd)
    rows = dict_fetchall(sql_cursor)
    if len(rows):
        wechat_message_rows = get_message_queue('c40',billno)
        insert_message_ctn(rows,"c40",billno,wechat_message_rows)


# 海关放行
def hgfx_out(billno):
    sql = sql_mes(billno,'')
    str_hgfx = sql.sql_hgfx_out()
    sql_cursor.execute(str_hgfx)
    rows = dict_fetchall(sql_cursor)
    if len(rows):
        wechat_message_rows = get_message_queue('c50',billno)
        insert_message_billno(rows,"c50",billno,wechat_message_rows)

# 船舶离港
def cblg(billno):
    sql = sql_mes(billno,'')
    str_cblg = sql.sql_cblg()
    oracle_cursor.execute(str_cblg)
    rows = dict_fetchall(oracle_cursor)
    if len(rows):
        wechat_msaage_rows = get_message_queue('c60',billno)
        insert_message_billno(rows,'c60',billno,wechat_msaage_rows)

# 到上海港

# 进口转关单已获取
def jkzgd(billno):
    sql = sql_mes(billno,'')
    str_cblg = sql.sql_zgdhq()
    sql_cursor.execute(str_cblg)
    rows = dict_fetchall(sql_cursor)
    if len(rows):
        wechat_msaage_rows = get_message_queue('j10',billno)
        insert_message_billno(rows,'j10',billno,wechat_msaage_rows)

# 卸船完工
def xcwg(billno):
    sql = sql_mes(billno,'')
    str_count = sql.sql_zgd_count()
    str_xcwg = sql.sql_xcwg()
    sql_cursor.execute(str_count)
    zgd_count = dict_fetchall(sql_cursor)
    oracle_cursor.execute(str_xcwg)
    rows = dict_fetchall(oracle_cursor)
    if len(rows)>0 and zgd_count[0]['NUM']>0:
        for row in rows:
            row['MES_CONTENT'] = row['MES_CONTENT']+','+zgd_count[0]['NUM']
        wechat_message_rows = get_message_queue('j20',billno)
        # mes_content内容
        insert_message_ctn(rows,'j20',billno,wechat_message_rows)


# 进口核销完成
def jkhx(billno):
    sql = sql_mes(billno,'')
    str_jkhx = sql.sql_jkhx()
    sql_cursor.execute(str_jkhx)
    rows = dict_fetchall(sql_cursor)
    if len(rows):
        wechat_message_rows = get_message_queue('j30',billno)
        insert_message_billno(rows,'j30',billno,wechat_message_rows)


# 海关放行
def hgfx_in(billno):
    sql = sql_mes(billno,'')
    str_hgfx = sql.sql_hgfx_in()
    sql_cursor.execute(str_hgfx)
    rows = dict_fetchall(sql_cursor)
    if len(rows):
        wechat_message_rows = get_message_queue('j40',billno)
        insert_message_billno(rows,'j40',billno,wechat_message_rows)


# 提重计划
def tzjh(billno):
    sql = sql_mes(billno,'')
    str_tzjh = sql.sql_tzjh()
    oracle_cursor.execute(str_tzjh)
    rows = dict_fetchall(oracle_cursor)
    if len(rows):
        wechat_message_rows = get_message_queue('j50',billno)
        insert_message_billno(rows,'j50',billno,wechat_message_rows)


# 提箱出场
def txcc(billno):
    sql = sql_mes(billno,'')
    str_txcc = sql.sql_txcc()
    oracle_cursor.execute(str_txcc)
    rows = dict_fetchall(oracle_cursor)
    if len(rows):
        wechat_msaage_rows = get_message_queue('j60',billno)
        insert_message_ctn(rows,'j60',billno,wechat_msaage_rows)
