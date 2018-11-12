import cx_Oracle
import pymssql
from setting import sqlconfig
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

oracle_con_string = sqlconfig.ORACLE_LJ
connect_oracle = cx_Oracle.connect(oracle_con_string)


# oracle数据库连接
def con_oracle():
    global connect_oracle
    if connect_oracle:
        return connect_oracle
    else:
        connect_oracle = cx_Oracle.connect(oracle_con_string)
        return connect_oracle

sql_con_string = sqlconfig.SQL_LJ
connect_sql = pymssql.connect(sql_con_string['server'], sql_con_string["user"], sql_con_string["password"], sql_con_string["database"])
# sqlservel数据库连接
def con_sql():
    global connect_sql
    if connect_sql:
        return connect_sql
    else:
        connect_sql = pymssql.connect(sql_con_string['server'], sql_con_string["user"], sql_con_string["password"], sql_con_string["database"])
        return connect_sql

# 获取游标
def cursor(con):
    cursor = con.cursor()
    return cursor