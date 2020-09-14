import datetime
import multiprocessing as mp
import re
import traceback

import cx_Oracle

import log
from environment import *
from scr.dao import ran_baseline_oracle
from scr.helper import naming_helper
from scr.helper.naming_helper import START_PARSING_STATEMENT, PREPARING_TABLE_STATEMENT, PARSING_FILE_STATEMENT

xml_namespaces = {'schemaLocation': 'http://www.ERICSSON.com/specs/ERICSSON_wl_bulkcm_xml_baseline_syn_1.0.0',
                  'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
                  'spec': 'http://www.ERICSSON.com/specs/ERICSSON_wl_bulkcm_xml_baseline_syn_1.0.0'}

VALUE_PAIR_SEPARATOR = ';'
PARAM_VALUE_ASSIGNER = ':'

KEY_BSC_LEVEL = "BSC Level"
KEY_CELL_LEVEL = "CELL Level"

KEY_SYSOBJECTID = "SYSOBJECTID"
KEY_CELLID = "CELLID"
KEY_CELLNAME = "CELLNAME"
KEY_NENAME = "NENAME"
KEY_LOCALCELLID = "LocalCellId"

KEY_REFERENCE_FIELD = "REFERENCE_FIELD"
KEY_MO_FIELD = "MO"
KEY_FILENAME_FIELD = "FILENAME"

KEY_TABLE = "{0}_{1}_{2}"

SYSOBJECTID = ""

CNA = 'CNA'

REGEX_UTRANCELL = r",UtranCell=(.*?),"
REGEX_MECONTEXT = r",MeContext=(.*?),"
REGEX_EUTRANCELLFDD = r",EUtranCellFDD=(.*?),"
REGEX_SW_NAME_3G = '^MO.*,MeContext=(.*),SwManagement=1,ConfigurationVersion=1$'
REGEX_SW_NAME_4G_2 = '^MO.*,MeContext=(.*),SystemFunctions=1,BrM=1,BrmBackupManager=1,BrmBackup=(.*)'
REGEX_NETYPENAME = '^MO.*,MeContext=(.*),ManagedElement=1$'
REGEX_UPGRADEPACKAGE = 'UpgradePackage=(.*?)$'

LV_FIELD = "LV"

sw_column = [
    "NAME",
    "FAMILYTYPE",
    "REFERENCE_FIELD",
    "SWVERSION",
    "FILENAME",
    "NETYPENAME",
    "NEFUNCTION"
]


def close_connection(connection, cur):
    cur.close()
    connection.close()


def open_connection():
    dsn_tns = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, ORACLE_SID)
    connection = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, dsn_tns)
    cur = connection.cursor()
    return connection, cur


def prepare_oracle_table(oracle_con, oracle_cur, frequency_type, field_mapping_dic, base_mapping_dic):
    log.i(PREPARING_TABLE_STATEMENT + " : " + frequency_type)

    if (ERICSSON_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
        ran_baseline_oracle.drop(oracle_cur, "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type)
        ran_baseline_oracle.create_table(oracle_cur, "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type, sw_column)

    for group_param in field_mapping_dic:
        table_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX, frequency_type, group_param)
        column_collection = field_mapping_dic[group_param]

        if (ERICSSON_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, table_name)
            ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

    for group_param in field_mapping_dic:
        table_name = naming_helper.get_table_name(BASELINE_TABLE_PREFIX.format(ERICSSON_TABLE_PREFIX), frequency_type, group_param)
        column_collection = field_mapping_dic[group_param]

        if (ERICSSON_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, table_name)
            ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_dic[group_param])

    CREATED_TABLE[ERICSSON_TABLE_PREFIX + "_" + frequency_type] = []
    log.i("Prepare_oracle_table End : CREATED_TABLE : " + str(CREATED_TABLE))

    oracle_con.commit()

    return


def prepare_oracle_table_4g(oracle_con, oracle_cur, frequency_type, field_mapping_dic, base_mapping_900_dic, base_mapping_1800_dic, base_mapping_2100_dic):
    log.i(PREPARING_TABLE_STATEMENT + " : " + frequency_type)

    if (ERICSSON_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
        ran_baseline_oracle.drop(oracle_cur, "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type)
        ran_baseline_oracle.create_table(oracle_cur, "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type, sw_column)

    for group_param in field_mapping_dic:
        table_name = naming_helper.get_table_name(BASELINE_TABLE_PREFIX.format(ERICSSON_TABLE_PREFIX), frequency_type, group_param)
        column_collection = field_mapping_dic[group_param]

        if (ERICSSON_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, table_name)
            ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

            try:
                ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_900_dic[group_param])
                ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_1800_dic[group_param])
                ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_2100_dic[group_param])
            except:
                traceback.print_exc()
                pass

    for group_param in field_mapping_dic:
        table_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX, frequency_type, group_param)
        column_collection = field_mapping_dic[group_param]

        if (ERICSSON_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, table_name)
            ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

    CREATED_TABLE[ERICSSON_TABLE_PREFIX + "_" + frequency_type] = []
    oracle_con.commit()


def run(source, field_mapping_dic, param_cell_level_dic, param_mo_dic):
    log.i(START_PARSING_STATEMENT.format(source.FrequencyType, source.Region))

    pool = mp.Pool(processes=MAX_RUNNING_PROCESS)

    for raw_file in source.RawFileList:

        if source.FrequencyType == '2G':
            # pool.apply_async(parse_2g, args=(raw_file, source.FrequencyType, field_mapping_dic,))
            continue
        else:
            pool.apply_async(parse_3g_4g, args=(raw_file, source.FrequencyType, field_mapping_dic, param_cell_level_dic, param_mo_dic,))

    pool.close()
    pool.join()

    print("Done -------Pool ALL")


def parse_3g_4g(raw_file, frequency_type, field_mapping_dic, param_cell_level_dic, param_mo_dic):
    log.i(PARSING_FILE_STATEMENT.format(raw_file))
    log.i("----- Start Parser : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    oracle_con, oracle_cur = open_connection()

    mongo_result = {}
    oracle_result = {}

    filename_dic = raw_file.split("/")

    filename = filename_dic[len(filename_dic) - 1]

    ne_name = ""

    sw_name = ""
    sw_version = ""
    sw_familytype = "DU"
    sw_nefunction = ""
    sw_netypename = ""

    with open(raw_file) as f:
        lines = f.readlines()

        mo_dics = find_mo_to_mem(lines)

        for group_param in param_mo_dic:
            group_param = group_param.upper()
            reg_mo_str = param_mo_dic[group_param]
            reg_mo_dic = reg_mo_str.split("\n")

            for reg_mo in reg_mo_dic:

                for line in mo_dics.keys():

                    index = mo_dics[line]

                    if group_param not in mongo_result:
                        mongo_result[group_param] = []
                        oracle_result[group_param] = []

                    if frequency_type == "3G" and (sw_version == "" or sw_netypename == ""):

                        matches = re.search(REGEX_SW_NAME_3G, line)

                        if matches:
                            sw_name = matches.group(1)
                            sw_name = sw_name.split(",")[0]

                            sw_version = find_swversion_3g(lines, index)

                        matches = re.search(REGEX_NETYPENAME, line)
                        if matches:
                            sw_netypename = find_netypename(lines, index)

                        sw_familytype = "DU"

                    elif frequency_type == "4G" and (sw_version == "" or sw_netypename == ""):

                        matches = re.search(REGEX_SW_NAME_3G, line)

                        if matches:
                            sw_name = matches.group(1)
                            sw_name = sw_name.split(",")[0]

                            sw_version = find_swversion_3g(lines, index)

                            sw_familytype = "DU"

                        matches = re.search(REGEX_SW_NAME_4G_2, line)

                        if matches:
                            sw_name = matches.group(1)
                            sw_name = sw_name.split(",")[0]

                            sw_version = find_swversion_4g(lines, index)

                            sw_familytype = "Baseband"

                        matches = re.search(REGEX_NETYPENAME, line)
                        if matches:
                            sw_netypename = find_netypename(lines, index)

    sw_result = {}
    dic = dict.fromkeys(sw_column, '')
    dic["NAME"] = sw_name
    dic["FAMILYTYPE"] = sw_familytype
    dic["SWVERSION"] = sw_version

    dic["NETYPENAME"] = sw_netypename
    dic["NEFUNCTION"] = sw_nefunction

    dic["FILENAME"] = filename
    dic["REFERENCE_FIELD"] = sw_name

    sw_key = "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type

    sw_result[sw_key] = []
    sw_result[sw_key].append(dic)

    ran_baseline_oracle.push(oracle_cur, sw_key, sw_result[sw_key])

    # log.i("----- Start mongo.push : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    # for group_param in mongo_result:
    #     collection_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX, frequency_type, group_param)
    # granite_mongo.push(collection_name, mongo_result[group_param])

    log.i("----- Start oracle.push : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    for group_param in oracle_result:
        try:
            collection_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX, frequency_type, group_param)
            ran_baseline_oracle.push(oracle_cur, collection_name, oracle_result[group_param])
            # oracle_con.commit()
        except:
            traceback.print_exc()
            pass

    oracle_con.commit()
    mongo_result.clear()
    oracle_result.clear()
    log.i("Done :::: " + filename + " ::::::::", ERICSSON_VENDOR, frequency_type)
    log.i("----- Parser done ----", ERICSSON_VENDOR, frequency_type)

    close_connection(oracle_con, oracle_cur)


def remove_word_inside_symbol(text, start_symbol, end_symbol, is_remove_symbol):
    replaced_text = str(text[text.index(start_symbol) + 1:text.rindex(end_symbol)])

    if is_remove_symbol:
        return text.replace(start_symbol + replaced_text + end_symbol, '')
    else:
        return text.replace(replaced_text, '')


def find_mo_to_mem(lines):
    mo_dics = {}
    for index, line in enumerate(lines):
        if line.startswith("MO      "):
            mo_dics[line] = index

    return mo_dics


def find_swversion_3g(lines, index):
    row = 2
    while True:
        dictData = lines[index + row].split()

        # End of MO
        if dictData[0][0] == "=":
            return ""

        key = dictData[0].upper()
        value = " ".join(dictData[1:])

        if key.upper() == "currentUpgradePackage".upper():

            matches = re.search(REGEX_UPGRADEPACKAGE, value)

            if matches:
                return matches.group(1)

        row += 1


def find_swversion_4g(lines, index):
    row = 2
    while True:
        dictData = lines[index + row].split()

        # End of MO
        if dictData[0][0] == "=":
            return ""

        key = dictData[0].upper()
        value = " ".join(dictData[1:])

        if key.upper() == "currentUpgradePackage".upper():

            matches = re.search(REGEX_UPGRADEPACKAGE, value)

            if matches:
                return matches.group(1)

        row += 1


def find_netypename(lines, index):
    row = 2
    while True:
        dictData = lines[index + row].split()

        # End of MO
        if dictData[0][0] == "=":
            return ""

        key = dictData[0].upper()
        value = " ".join(dictData[1:])

        if key.upper() == "productName".upper():
            return value

        row += 1
