#!/usr/bin/python3
import datetime

import cx_Oracle

import log
from environment import *
from scr.dao import ran_baseline_oracle
from scr.parser import main_baseline_parser

TIME_START_SCRIPT = datetime.datetime.now()
log.i(" ", HUAWEI_VENDOR, "4G")
log.i(" ", HUAWEI_VENDOR, "4G")
log.i("           ", HUAWEI_VENDOR, "4G")
log.i("Start Script : " + HUAWEI_VENDOR, HUAWEI_VENDOR, "4G")


def close_connection(connection, cur):
    cur.close()
    connection.close()


def open_connection():
    dsn_tns = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, ORACLE_SID)
    connection = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, dsn_tns)
    cur = connection.cursor()
    return connection, cur


main_baseline_parser.run(HUAWEI_VENDOR, "4G")

oracle_con, oracle_cur = open_connection()
ran_baseline_oracle.gen_txrx_cell(oracle_cur)
ran_baseline_oracle.gen_txrx_node(oracle_cur)

oracle_con.commit()

close_connection(oracle_con, oracle_cur)

TIME_END_SCRIPT = datetime.datetime.now()
log.i("Time : " + str(TIME_END_SCRIPT - TIME_START_SCRIPT), HUAWEI_VENDOR, "4G")
log.i("--------------------------------", HUAWEI_VENDOR, "4G")
log.i("--------------------------------", HUAWEI_VENDOR, "4G")
log.count()
log.i("--------------------------------", HUAWEI_VENDOR, "4G")
log.i("--------------------------------", HUAWEI_VENDOR, "4G")

log.i("Done all : " + HUAWEI_VENDOR + " 4G", HUAWEI_VENDOR, "4G")

log.i("           ", HUAWEI_VENDOR, "4G")
log.i("           ", HUAWEI_VENDOR, "4G")
log.i("           ", HUAWEI_VENDOR, "4G")
log.i("           ", HUAWEI_VENDOR, "4G")
log.i("           ", HUAWEI_VENDOR, "4G")
log.i("           ", HUAWEI_VENDOR, "4G")
