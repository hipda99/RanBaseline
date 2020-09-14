import glob

import cx_Oracle

import log
from environment import *
from scr.dao import ran_baseline_oracle
from scr.util.excel_reader import read_excel_mapping


log.i("Start Script")

def close_connection(connection, cur):
    cur.close()
    connection.close()


def open_connection():
    dsn_tns = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, ORACLE_SID)
    connection = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, dsn_tns)
    cur = connection.cursor()
    print ("Oracle version : " + connection.version)
    return connection, cur


connection, cur = open_connection()
close_connection(connection, cur)