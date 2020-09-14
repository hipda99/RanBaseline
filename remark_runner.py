#!/usr/bin/python3

import glob

import cx_Oracle

import log
from environment import *
from scr.dao import ran_baseline_oracle
from scr.util.excel_reader import read_excel_mapping


def close_connection(connection, cur):
    cur.close()
    connection.close()


def open_connection():
    dsn_tns = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, ORACLE_SID)
    connection = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, dsn_tns)
    cur = connection.cursor()
    print (connection.version)
    return connection, cur


def create_table_remark():
    oracle_con, oracle_cur = open_connection()

    collection_name = "REMARKS"
    ran_baseline_oracle.drop(oracle_cur, collection_name)
    ran_baseline_oracle.create_table(oracle_cur, collection_name, ["CELLNAME", "REMARK"])
    oracle_con.commit()


def run_remark(file_mapping_path_name):
    oracle_con, oracle_cur = open_connection()

    df = read_excel_mapping(file_mapping_path_name, 0)

    param_dic = {"REMARKS": []}
    log.i("Remark start  ")
    for index, row in df.iterrows():
        baseline_dic = {}
        cell_name = str(row["Cell Name"])
        remark = str(row["Remark"])

        baseline_dic["CELLNAME"] = cell_name
        baseline_dic["REMARK"] = remark

        param_dic["REMARKS"].append(baseline_dic)

        log.i("cell_name : " + cell_name + " : " + remark);

    collection_name = "REMARKS"
    ran_baseline_oracle.push(oracle_cur, collection_name, param_dic["REMARKS"])
    oracle_con.commit()

    log.i("DONE----")
    log.i("  ")
    log.i("  ")


create_table_remark()

run_remark("/home/app/ngoss/RanBaselineApp/BaselineFile/RemarkUpload/Remark.xlsx")