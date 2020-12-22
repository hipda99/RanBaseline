import datetime
import multiprocessing as mp
import re
import traceback
from timeit import default_timer as timer

import cx_Oracle

import log
from environment import *
from scr.dao import ran_baseline_oracle
from scr.helper import naming_helper
from scr.helper.mapping_helper import ericsson_mapping_helper
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
REGEX_NRCELLCU = r",NRCellCU=([^,]*)"
REGEX_NRCELLDU = r",NRCellDU=([^,]*)"
REGEX_EUTRANCELLFDD = r",EUtranCellFDD=(.*?),"
REGEX_EUTRANCELLTDD = r",EUtranCellTDD=(.*?),"
REGEX_SW_NAME_3G_OR_4G = '^MO.*,MeContext=(.*),SwManagement=1,ConfigurationVersion=1$'
REGEX_SW_NAME_4G_2 = '^MO.*,MeContext=(.*),SystemFunctions=1,BrM=1,BrmBackupManager=1,BrmBackup=(.*)'
REGEX_SW_NAME_3G_2 = '^MO.*,MeContext=(.*),SystemFunctions=1,SwInventory=1,SwVersion=(.*)'

REGEX_SW_SWVERSION_3G_BASEBAND = '^MO.*SystemFunctions=1,SwInventory=1,SwVersion=(.*)'
REGEX_SW_SWVERSION_2G_BASEBAND = '^MO.*,ManagedElement=(.*),BtsFunction=1,GsmSector=[^,]*$'

REGEX_SW_SWVERSION_2G_NAME = '^MO.*,ManagedElement=[^,]*$'

REGEX_SW_NETYPENAME = '^MO.*,MeContext=(.*),ManagedElement=1$'
REGEX_SW_NETYPENAME_BASEBAND = '^MO.*,MeContext=(.*),Equipment=1,FieldReplaceableUnit=1$'

REGEX_UPGRADEPACKAGE = 'UpgradePackage=(.*?)$'
REGEX_PRODUCTNUMBER = 'productNumber = (.*?)$'
REGEX_PRODUCTNAME = 'productName = (.*?)$'
REGEX_PRODUCTREVISION = 'productRevision = (.*?)$'
REGEX_STATUS = 'status = (.*?)$'

REGEX_FEATURE_5G_BASEBAND = '^MO.*,MeContext=(.*),SystemFunctions=1,Lm=1,FeatureState=[^,]*$'

REGEX_FEATURE_4G_BASEBAND = '^MO.*,MeContext=(.*),SystemFunctions=1,Lm=1,FeatureState=[^,]*$'
REGEX_FEATURE_4G_DU = '^MO.*,MeContext=(.*),SystemFunctions=1,Licensing=1,OptionalFeatureLicense=[^,]*$'

REGEX_FEATURE_3G_RNC = '^MO.*,MeContext=(.*),Licensing=1,RncFeature=[^,]*$'
REGEX_FEATURE_3G_NodeB = '^MO.*,SubNetwork=(.*),MeContext=(.*),NodeBFunction=[^,]*$'
REGEX_FEATURE_3G_Baseband = '^MO.*,ManagedElement=(.*),SystemFunctions=1,Lm=1,FeatureState=[^,]*$'

REGEX_QCIPROFILEPREDEFINED = 'QciProfilePredefined=(.*?)$'

REGEX_SW_2G_FULLKGET_NAME = '^MO.*,ManagedElement=(.*),BtsFunction=1,GsmSector=[^,]*$'

LV_FIELD = "LV"

sw_column = [
    "NAME",
    "FAMILYTYPE",
    "REFERENCE_FIELD",
    "SWVERSION",
    "FILENAME",
    "NETYPENAME",
    "NEFUNCTION",
    "BTS",
    "STATE",
    "MO"
]

# feature_column = [
#     "NAME",
#     "REFERENCE_FIELD",
#     "FILENAME",

#     "KEY_ID",
#     "FEATURESTATE",
#     "LICENSESTATE",
#     "SERVICESTATE",
#     "DESCRIPTION"

# ]


def close_connection(connection, cur):
    cur.close()
    connection.close()


def open_connection():
    dsn_tns = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, ORACLE_SID)
    connection = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, dsn_tns)
    cur = connection.cursor()
    return connection, cur


def prepare_oracle_table(oracle_con, oracle_cur, frequency_type, field_mapping_dic, base_mapping_dic, drop_param=True, base_mapping_label_dic={}):
    log.i(PREPARING_TABLE_STATEMENT + " : " + frequency_type)

    for group_param in field_mapping_dic:
        table_name = naming_helper.get_table_name(BASELINE_TABLE_PREFIX.format(ERICSSON_TABLE_PREFIX), frequency_type, group_param)
        column_collection = field_mapping_dic[group_param]

        if (ERICSSON_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, table_name)
            ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_dic[group_param])
            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_label_dic[group_param])

    if drop_param:
        if (ERICSSON_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type)
            ran_baseline_oracle.create_table(oracle_cur, "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type, sw_column)

        for group_param in field_mapping_dic:
            table_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX, frequency_type, group_param)
            column_collection = field_mapping_dic[group_param]

            if (ERICSSON_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
                ran_baseline_oracle.drop(oracle_cur, table_name)
                ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

    CREATED_TABLE[ERICSSON_TABLE_PREFIX + "_" + frequency_type] = []
    log.i("Prepare_oracle_table End : CREATED_TABLE : " + str(CREATED_TABLE))

    oracle_con.commit()

    return


def prepare_oracle_table_4g(oracle_con, oracle_cur, frequency_type, field_mapping_dic, base_mapping_900_dic, base_mapping_1800_dic, base_mapping_2100_dic, base_mapping_2600_dic, base_mapping_900_anchor_dic, base_mapping_1800_anchor_dic, base_mapping_2100_anchor_dic, base_mapping_2600_anchor_dic, drop_param=True, base_mapping_label_dic={}):
    log.i(PREPARING_TABLE_STATEMENT + " : " + frequency_type)

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

                # L2600
                if (len(base_mapping_2600_dic) != 0):
                    ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_2600_dic[group_param])

                # Anchor
                if (len(base_mapping_900_anchor_dic) != 0):
                    ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_900_anchor_dic[group_param])
                if (len(base_mapping_1800_anchor_dic) != 0):
                    ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_1800_anchor_dic[group_param])
                if (len(base_mapping_2100_anchor_dic) != 0):
                    ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_2100_anchor_dic[group_param])
                if (len(base_mapping_2600_anchor_dic) != 0):
                    ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_2600_anchor_dic[group_param])

                ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_label_dic[group_param])

            except:
                traceback.print_exc()
                pass

    if drop_param:
        if (ERICSSON_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type)
            ran_baseline_oracle.create_table(oracle_cur, "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type, sw_column)

        for group_param in field_mapping_dic:
            table_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX, frequency_type, group_param)
            column_collection = field_mapping_dic[group_param]

            if (ERICSSON_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
                ran_baseline_oracle.drop(oracle_cur, table_name)
                ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

    CREATED_TABLE[ERICSSON_TABLE_PREFIX + "_" + frequency_type] = []

    oracle_con.commit()

    return

def prepare_oracle_table_5g(oracle_con, oracle_cur, frequency_type, field_mapping_dic, base_mapping_2600_dic, drop_param=True, base_mapping_label_dic={}):
    log.i(PREPARING_TABLE_STATEMENT + " : " + frequency_type)

    for group_param in field_mapping_dic:
        table_name = naming_helper.get_table_name(BASELINE_TABLE_PREFIX.format(ERICSSON_TABLE_PREFIX), frequency_type, group_param)
        column_collection = field_mapping_dic[group_param]

        if (ERICSSON_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, table_name)
            ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

            try:
                ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_2600_dic[group_param])

                ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_label_dic[group_param])

            except:
                traceback.print_exc()
                pass

    if drop_param:
        if (ERICSSON_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type)
            ran_baseline_oracle.create_table(oracle_cur, "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type, sw_column)

        for group_param in field_mapping_dic:
            table_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX, frequency_type, group_param)
            column_collection = field_mapping_dic[group_param]

            if (ERICSSON_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
                ran_baseline_oracle.drop(oracle_cur, table_name)
                ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

    CREATED_TABLE[ERICSSON_TABLE_PREFIX + "_" + frequency_type] = []

    oracle_con.commit()

    return


def prepare_oracle_table_feature(oracle_con, oracle_cur, frequency_type, field_mapping_dic, base_mapping_dic):
    log.i(PREPARING_TABLE_STATEMENT + " : " + frequency_type)

    table_name = BASELINE_TABLE_PREFIX_FEATURE.format(ERICSSON_TABLE_PREFIX) + "_" + frequency_type

    column_collection = field_mapping_dic

    if table_name not in CREATED_TABLE:

        ran_baseline_oracle.drop(oracle_cur, "FEATURE_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type)
        ran_baseline_oracle.create_table(oracle_cur, "FEATURE_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type, ericsson_mapping_helper.feature_column)

        ran_baseline_oracle.drop(oracle_cur, table_name)
        ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

        try:
            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_dic)

        except:
            traceback.print_exc()
            pass

    CREATED_TABLE[table_name] = []
    oracle_con.commit()
    return


def prepare_oracle_table_software(oracle_con, oracle_cur, frequency_type):
    log.i("SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type)

    if ("SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
        ran_baseline_oracle.drop(oracle_cur, "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type)
        ran_baseline_oracle.create_table(oracle_cur, "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type, sw_column)

    CREATED_TABLE["SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type] = []
    oracle_con.commit()


def run(source, field_mapping_dic, param_cell_level_dic, param_mo_dic):
    log.i(START_PARSING_STATEMENT.format(source.FrequencyType, source.Region))

    pool = mp.Pool(processes=MAX_RUNNING_PROCESS)

    for raw_file in source.RawFileList:
        if source.FrequencyType == '2G':
            pool.apply_async(parse_2g, args=(raw_file, source.FrequencyType, field_mapping_dic,))
        else:
            # This include 5G as well
            pool.apply_async(parse, args=(raw_file, source.FrequencyType, field_mapping_dic, param_cell_level_dic, param_mo_dic,))
            # parse(raw_file, source.FrequencyType, field_mapping_dic, param_cell_level_dic, param_mo_dic)

    pool.close()
    pool.join()

    print("Done -------Pool ALL")


def run_sw(source):
    log.i(START_PARSING_STATEMENT.format(source.FrequencyType, source.Region))

    pool = mp.Pool(processes=MAX_RUNNING_PROCESS)

    for raw_file in source.RawFileList:
        if source.FrequencyType == '3G' or source.FrequencyType == '4G' or source.FrequencyType == '5G':
            pool.apply_async(parse_sw_3g_4g, args=(raw_file, source.FrequencyType,))
            # parse_sw_3g_4g(raw_file, source.FrequencyType)
            # continue;
        else:
            if ".log" in raw_file:
                # parse_sw_2g_full_kget(raw_file, source.FrequencyType)
                pool.apply_async(parse_sw_2g_full_kget, args=(raw_file, source.FrequencyType,))
                # continue
            else:
                # parse_sw_2g(raw_file, source.FrequencyType)
                pool.apply_async(parse_sw_2g, args=(raw_file, source.FrequencyType,))
                # continue

    pool.close()
    pool.join()

    print("Done -------Pool ALL")


def parse_sw_2g_full_kget(raw_file, frequency_type):
    log.i(PARSING_FILE_STATEMENT.format(raw_file))
    log.i("----- Start Feature Parser : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    oracle_con, oracle_cur = open_connection()

    mongo_result = {}
    oracle_result = {}

    filename_dic = raw_file.split("/")

    filename = filename_dic[len(filename_dic) - 1]
    ne_name = ""

    encodings = ['utf-8', 'windows-1250', 'windows-1252']

    productNumber = ""
    productName = ""
    productStatus = ""

    productNumberTmp = ""
    productNameTmp = ""
    productStatusTmp = ""

    sw_key = "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type
    sw_result = {}
    sw_result[sw_key] = []

    for e in encodings:
        with open(raw_file, encoding=e, errors='ignore') as f:
            try:

                with open(raw_file) as f:
                    lines = f.readlines()

                    mo_dics = find_mo_to_mem(lines)
                    NAME = ""

                    for line in mo_dics.keys():
                        index = mo_dics[line]

                        matches = re.search(REGEX_SW_NAME_4G_2, line)
                        if matches:
                            productNumber, productName, productStatus = find_swversion_2g(lines, index)
                            productNumberTmp, productNameTmp, productStatusTmp = find_swversion_2g_tmp(lines, index)

                            if productNumber == productNumberTmp:
                                break

                            # print("hi")
                    for line in mo_dics.keys():
                        index = mo_dics[line]

                        matches = re.search(REGEX_SW_SWVERSION_2G_NAME, line)
                        if matches:
                            NAME = find_swversion_2g_name(lines, index)
                            break

                    for line in mo_dics.keys():
                        index = mo_dics[line]

                        matches = re.search(REGEX_SW_SWVERSION_2G_BASEBAND, line)
                        if matches:
                            NAME_BTS, MO, BTS = find_swversion_2g_bts(lines, index)

                            if productNumber == "":
                                productNumber = productNumberTmp

                            if productName == "":
                                productName = productNameTmp

                            if productStatus == "":
                                productStatus = productStatusTmp

                            if NAME == "":
                                NAME = NAME_BTS

                            dic = dict.fromkeys(sw_column, '')
                            dic["NAME"] = NAME
                            dic["FAMILYTYPE"] = productName
                            dic["SWVERSION"] = productNumber

                            dic["BTS"] = BTS
                            dic["STATE"] = productStatus
                            dic["MO"] = MO

                            dic["FILENAME"] = filename
                            dic["REFERENCE_FIELD"] = NAME

                            sw_result[sw_key].append(dic)

                            continue

            except UnicodeDecodeError:
                print('got unicode error with %s , trying different encoding' % e)

            else:
                print('opening the file with encoding:  %s ' % e)
                break

    # log.i("----- Start mongo.push : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    # for group_param in mongo_result:
    #     collection_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX, frequency_type, group_param)
    #     granite_mongo.push(collection_name, mongo_result[group_param])

    ran_baseline_oracle.push(oracle_cur, sw_key, sw_result[sw_key])
    oracle_con.commit()
    mongo_result.clear()
    oracle_result.clear()
    log.i("Done FEATURE :::: " + filename + " ::::::::", ERICSSON_VENDOR, frequency_type)
    log.i("----- Parser FEATURE done ----", ERICSSON_VENDOR, frequency_type)

    close_connection(oracle_con, oracle_cur)


def parse_sw_2g(raw_file, frequency_type):
    log.i(PARSING_FILE_STATEMENT.format(raw_file))
    log.i("----- Start Feature Parser : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    oracle_con, oracle_cur = open_connection()

    mongo_result = {}
    oracle_result = {}

    filename_dic = raw_file.split("/")

    filename = filename_dic[len(filename_dic) - 1]

    encodings = ['utf-8', 'windows-1250', 'windows-1252']

    identity = ""

    sw_key = "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type
    sw_result = {sw_key: []}

    for e in encodings:
        with open(raw_file, encoding=e, errors='ignore') as f:
            try:
                with open(raw_file) as f:
                    lines = f.readlines()
                    mo_dics = {}
                    is_start = False
                    for index, line in enumerate(lines):
                        if line.startswith("IDENTITY"):
                            identity = lines[index + 1].strip()
                        elif line.startswith("<RXMOP:MOTY=RXOTG;"):
                            is_start = True

                        elif line.startswith("MO") and is_start:

                            mo_data = lines[index + 1].split()

                            mo = mo_data[0]
                            rsite = mo_data[1]

                            ver_data = lines[index + 4].split()

                            swverdld = ver_data[0]
                            swveract = ver_data[1]

                            # print(lines[index + 1])

                            dic = dict.fromkeys(sw_column, '')
                            dic["NAME"] = identity
                            dic["REFERENCE_FIELD"] = rsite
                            dic["FILENAME"] = filename

                            dic["SWVERSION"] = swverdld

                            dic["NETYPENAME"] = ""
                            dic["NEFUNCTION"] = ""

                            dic["FAMILYTYPE"] = "DU"

                            dic["BTS"] = rsite
                            dic["MO"] = mo
                            dic["STATE"] = "OPER"
                            # dic["SWVERACT"] = swveract

                            sw_result[sw_key].append(dic)

                        elif line.startswith("END") and is_start:

                            break

                    ran_baseline_oracle.push(oracle_cur, sw_key, sw_result[sw_key])
                    oracle_con.commit()
                    # mongo_result.clear()
                    oracle_result.clear()
                    log.i("Done Software :::: " + filename + " ::::::::", ERICSSON_VENDOR, frequency_type)
                    log.i("----- Parser Software done ----", ERICSSON_VENDOR, frequency_type)

                    close_connection(oracle_con, oracle_cur)

            except UnicodeDecodeError:
                print('got unicode error with %s , trying different encoding' % e)

            else:
                print('opening the file with encoding:  %s ' % e)
                break


def run_feature(source, field_mapping_dic, key_dic):
    log.i(START_PARSING_STATEMENT.format(source.FrequencyType, source.Region))

    pool = mp.Pool(processes=MAX_RUNNING_PROCESS)

    for raw_file in source.RawFileList:
        if source.FrequencyType == '3G' or source.FrequencyType == '4G' or source.FrequencyType == '5G':
            pool.apply_async(parse_feature_3g_4g, args=(raw_file, source.FrequencyType, key_dic,))
            # parse_feature_3g_4g(raw_file, source.FrequencyType, key_dic)
        #
        else:
            pool.apply_async(parse_feature_2g, args=(raw_file, source.FrequencyType, field_mapping_dic, key_dic,))
            # parse_feature_2g(raw_file, source.FrequencyType, field_mapping_dic, key_dic)

    pool.close()
    pool.join()

    print("Done -------Pool ALL")


def parse_2g(raw_file, frequency_type, field_mapping_dic):
    log.i(PARSING_FILE_STATEMENT.format(raw_file))
    log.i("----- Start Parser : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)

    oracle_con, oracle_cur = open_connection()

    mongo_result = {}
    oracle_result = {}
    mongo_result[CNA] = []
    oracle_result[CNA] = []

    filename_dic = raw_file.split("/")

    filename = filename_dic[len(filename_dic) - 1]
    start_parser_time = datetime.datetime.now()

    log.i("filename : " + filename, ERICSSON_VENDOR)

    columns = []
    with open(raw_file) as f:
        for line_number, line in enumerate(f, 1):

            if '********' in line:
                return

            line = line.replace(' \n', '')
            line = line.replace('NULL', '')
            mongo_value_pair_dic = {}
            oracle_value_pair_dic = {}

            if line_number == 1:
                columns = line.split(' ')
                continue

            if line_number == 2:
                continue

            value_line = line.split(' ')

            for idx, val in enumerate(value_line):

                # 2020-12-04 - Since Developer not push to Mongo, comment below line to reserve memory
                # mongo_value_pair_dic[columns[idx].upper()] = val

                if columns[idx].upper() in field_mapping_dic['CNA']:
                    if columns[idx].upper() not in oracle_value_pair_dic:
                        oracle_value_pair_dic[columns[idx].upper()] = val

            if KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, "CNA") not in COUNT_DATA:
                COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, "CNA")] = 0
            COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, "CNA")] = COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, "CNA")] + 1

            # 2020-12-04 - Since Developer not push to Mongo, comment below line to reserve memory
            # mongo_value_pair_dic[KEY_REFERENCE_FIELD] = mongo_value_pair_dic['CELL']
            # mongo_value_pair_dic[KEY_FILENAME_FIELD] = filename

            oracle_value_pair_dic[KEY_REFERENCE_FIELD] = oracle_value_pair_dic['CELL']
            oracle_value_pair_dic[KEY_FILENAME_FIELD] = filename

            # 2020-12-04 - Since Developer not push to Mongo, comment below line to reserve memory
            # mongo_result[CNA].append(mongo_value_pair_dic)
            oracle_result[CNA].append(oracle_value_pair_dic)

    # log.i('---- pushing to mongo')
    # for result in mongo_result:
    #     table_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX, frequency_type, result)
    #     granite_mongo.push(table_name, mongo_result[result])

    log.i('---- pushing to oracle')
    for result in oracle_result:
        table_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX, frequency_type, result)
        try:
            ran_baseline_oracle.push(oracle_cur, table_name, oracle_result[result])

        except Exception as e:
            log.e('#################################### Error occur (003): ', ERICSSON_VENDOR)
            log.e('Exception Into Table: ' + table_name, ERICSSON_VENDOR)
            log.e(e, ERICSSON_VENDOR)
            log.e('#################################### Error ', ERICSSON_VENDOR)

            raise (e)

    log.i("Done :::: " + filename + " ::::::::", ERICSSON_VENDOR, frequency_type)
    log.i("<<<< Time : " + str(datetime.datetime.now() - start_parser_time), ERICSSON_VENDOR, frequency_type)

    oracle_con.commit()

    close_connection(oracle_con, oracle_cur)


def parse_feature_2g(raw_file, frequency_type, field_mapping_dic, key_dic):
    log.i(PARSING_FILE_STATEMENT.format(raw_file))
    log.i("----- Start Feature Parser : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    oracle_con, oracle_cur = open_connection()

    mongo_result = {}
    oracle_result = {}

    filename_dic = raw_file.split("/")

    filename = filename_dic[len(filename_dic) - 1]

    encodings = ['utf-8', 'windows-1250', 'windows-1252']

    identity = ""

    sw_key = "FEATURE_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type
    sw_result = {sw_key: []}

    for e in encodings:
        with open(raw_file, encoding=e, errors='ignore') as f:
            try:
                with open(raw_file) as f:
                    lines = f.readlines()
                    mo_dics = {}
                    is_start = False
                    for index, line in enumerate(lines):
                        if line.startswith("IDENTITY"):
                            identity = lines[index + 1].strip()
                        elif line.startswith("<DBTSP:TAB=AXEPARS;"):
                            is_start = True

                        elif line.startswith("NAME") and is_start:
                            data = lines[index + 1].split()

                            key_id = data[0]
                            featurestate = data[3]
                            description = ''

                            if key_id in key_dic:
                                print(lines[index + 1])

                                dic = dict.fromkeys(ericsson_mapping_helper.feature_column, '')
                                dic["NAME"] = identity
                                dic["REFERENCE_FIELD"] = identity
                                dic["FILENAME"] = filename

                                dic["DESCRIPTION"] = description
                                dic["FEATURESTATE"] = featurestate
                                dic["LICENSESTATE"] = ""
                                dic["SERVICESTATE"] = ""

                                dic["KEY_ID"] = key_id
                                dic["LV"] = "BASEBAND"

                                sw_result[sw_key].append(dic)

                        elif line.startswith("END") and is_start:

                            break

                    ran_baseline_oracle.push(oracle_cur, sw_key, sw_result[sw_key])
                    oracle_con.commit()
                    mongo_result.clear()
                    oracle_result.clear()
                    log.i("Done FEATURE :::: " + filename + " ::::::::", ERICSSON_VENDOR, frequency_type)
                    log.i("----- Parser FEATURE done ----", ERICSSON_VENDOR, frequency_type)

                    close_connection(oracle_con, oracle_cur)

            except UnicodeDecodeError:
                print('got unicode error with %s , trying different encoding' % e)

            else:
                print('opening the file with encoding:  %s ' % e)
                break


def parse_feature_3g(raw_file, frequency_type, field_mapping_dic):
    log.i(PARSING_FILE_STATEMENT.format(raw_file))
    log.i("----- Start Feature Parser : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    oracle_con, oracle_cur = open_connection()

    mongo_result = {}
    oracle_result = {}

    filename_dic = raw_file.split("/")

    filename = filename_dic[len(filename_dic) - 1]
    ne_name = ""

    encodings = ['utf-8', 'windows-1250', 'windows-1252']

    for e in encodings:
        with open(raw_file, encoding=e, errors='ignore') as f:
            try:

                with open(raw_file) as f:
                    lines = f.readlines()

                    mo_dics = find_mo_to_mem(lines)

                    for group_param in field_mapping_dic:
                        group_param = group_param.upper()

                        param_collection = field_mapping_dic[group_param]

                        oracle_value_pair_dic = dict.fromkeys(param_collection, '')

                        has_data = 0
                        for line in mo_dics.keys():

                            index = mo_dics[line]

                            if frequency_type == "3G":

                                if group_param == 'BASEBAND':
                                    matches = re.search(REGEX_FEATURE_3G_Baseband, line)

                                    if matches:
                                        ne_name = matches.group(1)
                                        ne_name = ne_name.split(",")[0]

                                        key_id, featurestate, description, servicestate, licensestate = find_feature_version_3g_4g(lines, index)
                                        key = naming_helper.rule_column_name(key_id)
                                        key = key.upper()

                                        if key in param_collection:
                                            oracle_value_pair_dic[key] = featurestate
                                            has_data = 1

                                elif group_param == 'RNC':
                                    matches = re.search(REGEX_FEATURE_3G_RNC, line)

                                    if matches:
                                        ne_name = matches.group(1)
                                        ne_name = ne_name.split(",")[0]

                                        key_id, featurestate, description, servicestate, licensestate = find_feature_version_3g_4g(lines, index)

                                        if description == '':
                                            continue

                                        key = naming_helper.rule_column_name(description)
                                        key = key.upper()

                                        if key in param_collection:
                                            oracle_value_pair_dic[key] = featurestate
                                            has_data = 1

                                elif group_param == 'NODEB':
                                    matches = re.search(REGEX_FEATURE_3G_NodeB, line)

                                    if matches:
                                        ne_name = matches.group(2)
                                        ne_name = ne_name.split(",")[0]

                                        # key_id, featurestate, description = find_feature_version_3g_4g(lines, index)
                                        row = 2
                                        is_run = True
                                        while is_run:
                                            dictData = lines[index + row].split()
                                            row += 1

                                            if dictData[0][0] == "=":
                                                is_run = False

                                            key = dictData[0]
                                            value = " ".join(dictData[1:])

                                            if key.upper().startswith("FEATURESTATE"):
                                                featurestate = value
                                                description = key

                                                key = naming_helper.rule_column_name(description)
                                                key = key.upper()

                                                if key in param_collection:
                                                    oracle_value_pair_dic[key] = featurestate
                                                    has_data = 1

                                        if has_data == 1:
                                            oracle_value_pair_dic[KEY_FILENAME_FIELD] = filename
                                            oracle_value_pair_dic[KEY_REFERENCE_FIELD] = ne_name

                                            oracle_result[group_param] = []
                                            oracle_result[group_param].append(oracle_value_pair_dic)

                                            has_data = 0
                            elif frequency_type == "4G":

                                matches = re.search(REGEX_FEATURE_4G_DU, line)

                                if matches:
                                    ne_name = matches.group(1)
                                    ne_name = ne_name.split(",")[0]

                                    key_id, featurestate, description, servicestate, licensestate = find_feature_version_3g_4g(lines, index)

                                    if key_id == '':
                                        continue

                                    key = naming_helper.rule_column_name(key_id)
                                    key = key.upper()

                                    if key in param_collection:
                                        oracle_value_pair_dic[key] = featurestate
                                        has_data = 1

                                matches = re.search(REGEX_FEATURE_4G_BASEBAND, line)

                                if matches:
                                    ne_name = matches.group(1)
                                    ne_name = ne_name.split(",")[0]

                                    key_id, featurestate, description, servicestate, licensestate = find_feature_version_3g_4g(lines, index)

                                    if key_id == '':
                                        continue

                                    key = naming_helper.rule_column_name(key_id)
                                    key = key.upper()

                                    if key in param_collection:
                                        oracle_value_pair_dic[key] = featurestate
                                        has_data = 1

                        if has_data == 1:
                            oracle_value_pair_dic[KEY_FILENAME_FIELD] = filename
                            oracle_value_pair_dic[KEY_REFERENCE_FIELD] = ne_name

                            oracle_result[group_param] = []
                            oracle_result[group_param].append(oracle_value_pair_dic)

                            has_data = 0

            except UnicodeDecodeError:
                print('got unicode error with %s , trying different encoding' % e)

            else:
                print('opening the file with encoding:  %s ' % e)
                break

    for group_param in oracle_result:
        try:
            collection_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX + "_FEATURE", frequency_type, group_param)
            ran_baseline_oracle.push(oracle_cur, collection_name, oracle_result[group_param])
            # oracle_con.commit()
        except:
            traceback.print_exc()
            pass
    # log.i("----- Start mongo.push : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    # for group_param in mongo_result:
    #     collection_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX, frequency_type, group_param)
    #     granite_mongo.push(collection_name, mongo_result[group_param])

    oracle_con.commit()
    mongo_result.clear()
    oracle_result.clear()
    log.i("Done FEATURE :::: " + filename + " ::::::::", ERICSSON_VENDOR, frequency_type)
    log.i("----- Parser FEATURE done ----", ERICSSON_VENDOR, frequency_type)

    close_connection(oracle_con, oracle_cur)


def parse_feature_3g_4g(raw_file, frequency_type, key_dic):
    log.i(PARSING_FILE_STATEMENT.format(raw_file))
    log.i("----- Start SW Parser : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    oracle_con, oracle_cur = open_connection()

    mongo_result = {}
    oracle_result = {}

    filename_dic = raw_file.split("/")

    filename = filename_dic[len(filename_dic) - 1]
    ne_name = ""
    sw_key = "FEATURE_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type
    sw_result = {}
    sw_result[sw_key] = []

    key_id = ''
    featurestate = ''
    description = ''

    with open(raw_file) as f:
        lines = f.readlines()

        if frequency_type == "4G":
            firstLine = lines[0]
            if 'wcdma' in firstLine:
                log.i("----- STOP FOUND wcdma : " + firstLine)
                return

            if 'mixedmode//WG' in firstLine:
                log.i("----- STOP FOUND mixedmode//WG : " + firstLine)
                return

            if '/nr/' in firstLine:
                log.i("----- STOP FOUND mixedmode//WG : " + firstLine)
                return

            #TODO: Mixmode 5G , still Unknown

        if frequency_type == "5G":
            firstLine = lines[0]
            if 'wcdma' in firstLine:
                log.i("----- STOP FOUND wcdma : " + firstLine)
                return

            if 'mixedmode//WG' in firstLine:
                log.i("----- STOP FOUND mixedmode//WG : " + firstLine)
                return

            if 'mixedmode//LWG/' in firstLine:
                log.i("----- STOP FOUND mixedmode//LWG : " + firstLine)
                return
            if 'mixedmode//LG/' in firstLine:
                log.i("----- STOP FOUND mixedmode//LG : " + firstLine)
                return
            if 'mixedmode//LW/' in firstLine:
                log.i("----- STOP FOUND mixedmode//LW : " + firstLine)
                return

        mo_dics = find_mo_to_mem(lines)

        for line in mo_dics.keys():

            index = mo_dics[line]

            if frequency_type == "3G":

                matches = re.search(REGEX_FEATURE_3G_RNC, line)

                if matches:
                    ne_name = matches.group(1)
                    ne_name = ne_name.split(",")[0]

                    key_id, featurestate, description, servicestate, licensestate = find_feature_version_3g_4g(lines, index)

                    if description in key_dic:
                        dic = dict.fromkeys(ericsson_mapping_helper.feature_column, '')
                        dic["NAME"] = ne_name
                        dic["REFERENCE_FIELD"] = ne_name
                        dic["FILENAME"] = filename

                        dic["DESCRIPTION"] = description
                        dic["FEATURESTATE"] = featurestate
                        dic["LICENSESTATE"] = servicestate
                        dic["SERVICESTATE"] = licensestate

                        dic["KEY_ID"] = key_id
                        dic["LV"] = "RNC"

                        sw_result[sw_key].append(dic)

                        key_id = ''
                        featurestate = ''
                        description = ''
                        servicestate = ''
                        licensestate = ''

                    continue

                matches = re.search(REGEX_FEATURE_3G_NodeB, line)

                if matches:
                    ne_name = matches.group(2)
                    ne_name = ne_name.split(",")[0]

                    # key_id, featurestate, description = find_feature_version_3g_4g(lines, index)
                    row = 2
                    is_run = True
                    while is_run:
                        dictData = lines[index + row].split()
                        row += 1

                        # End of MO
                        if dictData[0][0] == "=":
                            is_run = False

                        key = dictData[0]
                        value = " ".join(dictData[1:])

                        if key.upper().startswith("FEATURESTATE"):
                            featurestate = value
                            description = key
                            if key in key_dic:
                                key_id = key.replace("featureState", "")

                                dic = dict.fromkeys(ericsson_mapping_helper.feature_column, '')
                                dic["NAME"] = ne_name
                                dic["REFERENCE_FIELD"] = ne_name
                                dic["FILENAME"] = filename

                                dic["DESCRIPTION"] = description
                                dic["FEATURESTATE"] = featurestate
                           

                                dic["KEY_ID"] = key_id
                                dic["LV"] = "NODEB"

                                sw_result[sw_key].append(dic)

                                key_id = ''
                                featurestate = ''
                                description = ''
                                servicestate = ''
                                licensestate = ''
                    continue

                matches = re.search(REGEX_FEATURE_3G_Baseband, line)

                if matches:
                    ne_name = matches.group(1)
                    ne_name = ne_name.split(",")[0]

                    key_id, featurestate, description, servicestate, licensestate = find_feature_version_3g_4g(lines, index)

                    if key_id in key_dic:
                        dic = dict.fromkeys(ericsson_mapping_helper.feature_column, '')
                        dic["NAME"] = ne_name
                        dic["REFERENCE_FIELD"] = ne_name
                        dic["FILENAME"] = filename

                        dic["DESCRIPTION"] = description
                        dic["FEATURESTATE"] = featurestate
                        dic["LICENSESTATE"] = licensestate
                        dic["SERVICESTATE"] = servicestate
                        

                        dic["KEY_ID"] = key_id
                        dic["LV"] = "BASEBAND"

                        sw_result[sw_key].append(dic)

                        key_id = ''
                        featurestate = ''
                        description = ''
                        servicestate = ''
                        licensestate = ''

                    continue

            elif frequency_type == "4G":

                matches = re.search(REGEX_FEATURE_4G_DU, line)

                if matches:
                    ne_name = matches.group(1)
                    ne_name = ne_name.split(",")[0]

                    key_id, featurestate, description, servicestate, licensestate = find_feature_version_3g_4g(lines, index)

                    if key_id != '' and featurestate != '':

                        if key_id in key_dic:
                            dic = dict.fromkeys(ericsson_mapping_helper.feature_column, '')
                            dic["NAME"] = ne_name
                            dic["REFERENCE_FIELD"] = ne_name
                            dic["FILENAME"] = filename

                            dic["DESCRIPTION"] = description
                            dic["FEATURESTATE"] = featurestate
                            dic["LICENSESTATE"] = licensestate
                            dic["SERVICESTATE"] = servicestate

                            dic["KEY_ID"] = key_id
                            dic["LV"] = "DU"
                            sw_result[sw_key].append(dic)

                            key_id = ''
                            featurestate = ''
                            description = ''
                            servicestate = ''
                            licensestate = ''

                    continue

                matches = re.search(REGEX_FEATURE_4G_BASEBAND, line)

                if matches:
                    ne_name = matches.group(1)
                    ne_name = ne_name.split(",")[0]

                    key_id, featurestate, description, servicestate, licensestate = find_feature_version_3g_4g(lines, index)

                    if key_id != '' and featurestate != '':

                        if key_id in key_dic:
                            dic = dict.fromkeys(ericsson_mapping_helper.feature_column, '')
                            dic["NAME"] = ne_name
                            dic["REFERENCE_FIELD"] = ne_name
                            dic["FILENAME"] = filename

                            dic["DESCRIPTION"] = description
                            dic["FEATURESTATE"] = featurestate
                            dic["LICENSESTATE"] = licensestate
                            dic["SERVICESTATE"] = servicestate

                            dic["KEY_ID"] = key_id
                            dic["LV"] = "BASEBAND"

                            sw_result[sw_key].append(dic)

                            key_id = ''
                            featurestate = ''
                            description = ''
                            servicestate = ''
                            licensestate = ''

            elif frequency_type == "5G":

                matches = re.search(REGEX_FEATURE_5G_BASEBAND, line)

                if matches:
                    ne_name = matches.group(1)
                    ne_name = ne_name.split(",")[0]

                    key_id, featurestate, description, servicestate, licensestate = find_feature_version_3g_4g(lines, index)

                    if key_id != '' and featurestate != '':

                        if key_id in key_dic:
                            dic = dict.fromkeys(ericsson_mapping_helper.feature_column, '')
                            dic["NAME"] = ne_name
                            dic["REFERENCE_FIELD"] = ne_name
                            dic["FILENAME"] = filename

                            dic["DESCRIPTION"] = description
                            dic["FEATURESTATE"] = featurestate
                            dic["LICENSESTATE"] = licensestate
                            dic["SERVICESTATE"] = servicestate

                            dic["KEY_ID"] = key_id
                            dic["LV"] = "BASEBAND"

                            sw_result[sw_key].append(dic)

                            key_id = ''
                            featurestate = ''
                            description = ''
                            servicestate = ''
                            licensestate = ''

    # log.i("----- Start mongo.push : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    # for group_param in mongo_result:
    #     collection_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX, frequency_type, group_param)
    #     granite_mongo.push(collection_name, mongo_result[group_param])

    ran_baseline_oracle.push(oracle_cur, sw_key, sw_result[sw_key])
    oracle_con.commit()
    mongo_result.clear()
    oracle_result.clear()
    log.i("Done FEATURE :::: " + filename + " ::::::::", ERICSSON_VENDOR, frequency_type)
    log.i("----- Parser FEATURE done ----", ERICSSON_VENDOR, frequency_type)

    close_connection(oracle_con, oracle_cur)


def parse_sw_3g_4g(raw_file, frequency_type):
    log.i(PARSING_FILE_STATEMENT.format(raw_file))
    log.i("----- Start SW Parser : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    oracle_con, oracle_cur = open_connection()

    mongo_result = {}
    oracle_result = {}

    filename_dic = raw_file.split("/")

    filename = filename_dic[len(filename_dic) - 1]

    sw_name = ""
    sw_version = ""
    sw_version_tmp = ""
    sw_familytype = "DU"
    sw_nefunction = ""
    sw_netypename = ""

    with open(raw_file) as f:
        lines = f.readlines()

        if frequency_type == "4G":
            firstLine = lines[0]
            if 'wcdma' in firstLine:
                log.i("----- STOP FOUND wcdma : " + firstLine)
                return

            if 'mixedmode//WG' in firstLine:
                log.i("----- STOP FOUND mixedmode//WG : " + firstLine)
                return

            if '/nr/' in firstLine:
                log.i("----- STOP FOUND mixedmode//WG : " + firstLine)
                return

            #TODO: Mixmode 5G , still Unknown

        if frequency_type == "5G":
            firstLine = lines[0]
            if 'wcdma' in firstLine:
                log.i("----- STOP FOUND wcdma : " + firstLine)
                return

            if 'mixedmode//WG' in firstLine:
                log.i("----- STOP FOUND mixedmode//WG : " + firstLine)
                return

            if 'mixedmode//LWG/' in firstLine:
                log.i("----- STOP FOUND mixedmode//LWG : " + firstLine)
                return
            if 'mixedmode//LG/' in firstLine:
                log.i("----- STOP FOUND mixedmode//LG : " + firstLine)
                return
            if 'mixedmode//LW/' in firstLine:
                log.i("----- STOP FOUND mixedmode//LW : " + firstLine)
                return

        mo_dics = find_mo_to_mem(lines)

        for line in mo_dics.keys():

            index = mo_dics[line]

            if sw_version != "" and sw_netypename != "":
                break

            if frequency_type == "3G" and (sw_version == "" or sw_netypename == ""):

                matches = re.search(REGEX_SW_NAME_3G_OR_4G, line)

                if matches:
                    sw_familytype = "DU"
                    sw_name = matches.group(1)
                    sw_name = sw_name.split(",")[0]

                    sw_version = find_swversion_3g(lines, index)

                    continue

                matches = re.search(REGEX_SW_NAME_3G_2, line)

                if matches:
                    sw_familytype = "Baseband"
                    sw_name = matches.group(1)
                    sw_name = sw_name.split(",")[0]

                    matches = re.search(REGEX_SW_SWVERSION_3G_BASEBAND, line)

                    if matches:
                        sw_version = matches.group(1)

                    continue

                matches = re.search(REGEX_SW_NETYPENAME, line)
                if matches:
                    sw_netypename = find_netypename(lines, index)

                    continue

                matches = re.search(REGEX_SW_NETYPENAME_BASEBAND, line)
                if matches:
                    sw_netypename = find_netypename_baseband(lines, index)
                    continue

            elif frequency_type == "4G" and (sw_version == "" or sw_netypename == ""):

                matches = re.search(REGEX_SW_NAME_3G_OR_4G, line)

                if matches:
                    sw_name = matches.group(1)
                    sw_name = sw_name.split(",")[0]

                    sw_version = find_swversion_3g(lines, index)

                    sw_familytype = "DU"
                    continue

                matches = re.search(REGEX_SW_NAME_4G_2, line)

                if matches and sw_version == '':
                    sw_name = matches.group(1)
                    sw_name = sw_name.split(",")[0]

                    sw_version = find_swversion_4g(lines, index)

                    sw_version_tmp = find_swversion_4g_tmp(lines, index)

                    sw_familytype = "Baseband"
                    continue

                matches = re.search(REGEX_SW_NETYPENAME_BASEBAND, line)
                if matches and sw_netypename == '':
                    sw_netypename = find_netypename_baseband(lines, index)
                    continue

                matches = re.search(REGEX_SW_NETYPENAME, line)
                if matches and sw_netypename == '':
                    sw_netypename = find_netypename(lines, index)
                    continue

            elif frequency_type == "5G" and (sw_version == "" or sw_netypename == ""):

                matches = re.search(REGEX_SW_NAME_4G_2, line)

                if matches and sw_version == '':
                    sw_name = matches.group(1)
                    sw_name = sw_name.split(",")[0]

                    sw_version = find_swversion_4g(lines, index)

                    sw_version_tmp = find_swversion_4g_tmp(lines, index)

                    sw_familytype = "Baseband"
                    continue

                matches = re.search(REGEX_SW_NETYPENAME_BASEBAND, line)
                if matches and sw_netypename == '':
                    sw_netypename = find_netypename_baseband(lines, index)
                    continue

                matches = re.search(REGEX_SW_NETYPENAME, line)
                if matches and sw_netypename == '':
                    sw_netypename = find_netypename(lines, index)
                    continue

    if sw_version == "" and sw_version_tmp != "":
        sw_version = sw_version_tmp

    if '%' in sw_version:
        sw_version = sw_version.replace("%", "/")

    if sw_netypename.startswith("RNC"):
        sw_familytype = "Evo Controller"

    sw_result = {}
    dic = dict.fromkeys(sw_column, '')
    dic["NAME"] = sw_name
    dic["FAMILYTYPE"] = sw_familytype
    dic["SWVERSION"] = sw_version

    sw_netypename = sw_netypename.strip()
    if sw_familytype == "Baseband":
        if sw_netypename.startswith("Baseband") or sw_netypename.startswith("DUS"):
            sw_netypename = "RBS6601"

    dic["NETYPENAME"] = sw_netypename
    dic["NEFUNCTION"] = sw_nefunction

    dic["FILENAME"] = filename
    dic["REFERENCE_FIELD"] = sw_name

    sw_key = "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type

    sw_result[sw_key] = []
    sw_result[sw_key].append(dic)

    log.i("----- Start oracle.push : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    ran_baseline_oracle.push(oracle_cur, sw_key, sw_result[sw_key])

    # log.i("----- Start mongo.push : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    # for group_param in mongo_result:
    #     collection_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX, frequency_type, group_param)
    #     granite_mongo.push(collection_name, mongo_result[group_param])

    oracle_con.commit()
    mongo_result.clear()
    oracle_result.clear()
    log.i("Done SW :::: " + filename + " ::::::::", ERICSSON_VENDOR, frequency_type)
    log.i("----- Parser SW done ----", ERICSSON_VENDOR, frequency_type)

    close_connection(oracle_con, oracle_cur)


def parse(raw_file, frequency_type, field_mapping_dic, param_cell_level_dic, param_mo_dic):
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
    qciProfilePredefinedId = ""

    with open(raw_file) as f:
        t = timer()
        lines = f.readlines()

        if frequency_type == "4G":
            firstLine = lines[0]
            if 'wcdma' in firstLine:
                log.i("----- STOP FOUND wcdma : " + firstLine)
                return

            if 'mixedmode//WG' in firstLine:
                log.i("----- STOP FOUND mixedmode//WG : " + firstLine)
                return

            if '/nr/' in firstLine:
                log.i("----- STOP FOUND mixedmode//WG : " + firstLine)
                return

        #TODO: Mixmode 5G , still Unknown

        if frequency_type == "5G":
            firstLine = lines[0]
            if 'wcdma' in firstLine:
                log.i("----- STOP FOUND wcdma : " + firstLine)
                return

            if 'mixedmode//WG' in firstLine:
                log.i("----- STOP FOUND mixedmode//WG : " + firstLine)
                return

            if 'mixedmode//LWG/' in firstLine:
                log.i("----- STOP FOUND mixedmode//LWG : " + firstLine)
                return
            if 'mixedmode//LG/' in firstLine:
                log.i("----- STOP FOUND mixedmode//LG : " + firstLine)
                return
            if 'mixedmode//LW/' in firstLine:
                log.i("----- STOP FOUND mixedmode//LW : " + firstLine)
                return

        mo_dics = find_mo_to_mem(lines)

        for group_param in param_mo_dic:
            group_param = group_param.upper()
            reg_mo_str = param_mo_dic[group_param]

            if not reg_mo_str or 'NO AUDIT' in reg_mo_str.upper(): # Group is not audit
                log.i(f"----- Skip group {group_param} reg_mo = {reg_mo_str}")
                continue

            reg_mo_dic = reg_mo_str.split("\n")

            for reg_mo in reg_mo_dic:

                reg_match = re.compile(reg_mo)

                reference_field = ""
                for line in mo_dics.keys():

                    index = mo_dics[line]

                    if group_param not in mongo_result:
                        mongo_result[group_param] = []
                        oracle_result[group_param] = []

                    group_level = param_cell_level_dic[group_param]
                    param_collection = field_mapping_dic[group_param]

                    line_match = reg_match.findall(line)

                    mongo_value_pair_dic = {}
                    oracle_value_pair_dic = dict.fromkeys(param_collection, '')

                    if len(line_match) > 0:

                        mo = line.replace("MO", "").strip()

                        if group_param == 'QCIPROFILEPREDEFINED':
                            matches = re.search(REGEX_QCIPROFILEPREDEFINED, mo)

                            if matches:
                                qciProfilePredefinedId = matches.group(1)

                        if group_param == 'COMMONBEAMFORMING' and frequency_type == '5G':
                            sectorIndex = re.search(r".*,NRSectorCarrier=([^,]*),.*", mo)
                            sector = None
                            if (sectorIndex):
                                sector = sectorIndex.group(1).strip()
                            regex = f'^MO.*,MeContext=(.*),GNBDUFunction=1,NRSectorCarrier={sector}$'
                            break_loop = False
                            for l_ in mo_dics.keys():
                                matches = re.search(regex, l_)
                                if matches:
                                    idx_ = mo_dics[l_]
                                    row = 2
                                    
                                    while True:
                                        dictData = lines[idx_ + row].split()
                                        if break_loop:
                                            break
                                        # End of MO
                                        if dictData[0][0] == "=":
                                            break

                                        if dictData[0][:11] == "reservedBy[":
                                            size_number = dictData[0].split('[')
                                            size_number = size_number[1].split(']')
                                            size_number = int(size_number[0])

                                            list_ = []
                                            row = row + 1

                                            for i in range(size_number):
                                                reserved_dict = lines[idx_ + row].split()

                                                value = reserved_dict[3]
                                                list_.append(value)
                                                row = row + 1
                                                if "NRCellDU" in value:
                                                    tmp_full_dic = value.split(",")
                                                    for tmp_dic in tmp_full_dic:
                                                        if "NRCellDU" in tmp_dic:
                                                            tmp_dic = tmp_dic.split("=")
                                                            reference_field = tmp_dic[1]
                                                            break_loop = True
                                                            break

                                                elif "NRCellCU" in value:
                                                    tmp_full_dic = value.split(",")
                                                    for tmp_dic in tmp_full_dic:
                                                        if "NRCellCU" in tmp_dic:
                                                            tmp_dic = tmp_dic.split("=")
                                                            reference_field = tmp_dic[1]
                                                            break_loop = True
                                                            break

                                        row += 1
                                    break
                                else:
                                    continue
                            if not reference_field:
                                reference_field = ne_name

                        elif group_level == "NE-RNC" or group_level == "NE-NodeB" or group_level == "NE":

                            if group_param == "IubLink".upper():
                                tmp_dic = mo.split("=")
                                reference_field = tmp_dic[len(tmp_dic) - 1]

                            elif ne_name == "":
                                matches = re.search(REGEX_MECONTEXT, mo)

                                if matches:
                                    ne_name = matches.group(1)

                                reference_field = ne_name
                            else:
                                reference_field = ne_name

                        elif group_level == "CELL":

                            if group_param == "ExternalGsmCell".upper() or group_param == "ExternalUtranCell".upper() or group_param == "UtranCell".upper() or group_param == "EUtranCellFDD".upper() or group_param == "EUtranCellTDD".upper():
                                tmp_dic = mo.split("=")
                                reference_field = tmp_dic[len(tmp_dic) - 1]

                            else:
                                matches = False
                                if frequency_type == "4G":
                                    if 'EUtranCellFDD' in mo:
                                        matches = re.search(REGEX_EUTRANCELLFDD, mo)
                                    elif 'EUtranCellTDD' in mo:
                                        matches = re.search(REGEX_EUTRANCELLTDD, mo)
                                elif frequency_type == "5G":
                                    if 'NRCELLDU'.upper() in group_param or 'NRCellDU' in mo:
                                        matches = re.search(REGEX_NRCELLDU, mo)
                                    elif 'NRCELLCU'.upper() in group_param or 'NRCellCU' in mo:
                                        matches = re.search(REGEX_NRCELLCU, mo)

                                else:
                                    matches = re.search(REGEX_UTRANCELL, mo)

                                if matches:
                                    reference_field = matches.group(1)
                                else:
                                    reference_field = ne_name

                        if KEY_TABLE.format(ERICSSON_TABLE_PREFIX, frequency_type, group_param) not in COUNT_DATA:
                            COUNT_DATA[KEY_TABLE.format(ERICSSON_TABLE_PREFIX, frequency_type, group_param)] = 0
                        COUNT_DATA[KEY_TABLE.format(ERICSSON_TABLE_PREFIX, frequency_type, group_param)] = COUNT_DATA[KEY_TABLE.format(ERICSSON_TABLE_PREFIX, frequency_type, group_param)] + 1

                        # 2020-12-04 - Since Developer not push to Mongo, comment below line to reserve memory
                        # mongo_value_pair_dic[KEY_FILENAME_FIELD] = filename
                        # mongo_value_pair_dic[KEY_MO_FIELD] = mo
                        # mongo_value_pair_dic[KEY_REFERENCE_FIELD] = reference_field
                        # mongo_value_pair_dic[LV_FIELD] = group_level

                        oracle_value_pair_dic[KEY_FILENAME_FIELD] = filename
                        oracle_value_pair_dic[KEY_MO_FIELD] = mo
                        oracle_value_pair_dic[KEY_REFERENCE_FIELD] = reference_field
                        oracle_value_pair_dic[LV_FIELD] = group_level

                        row = 2

                        while True:
                            dictData = lines[index + row].split()

                            if group_param == 'RNCFEATURE':

                                if dictData.__len__() > 1 and dictData[1].upper() in param_collection:
                                    key = naming_helper.rule_column_name(dictData[1])
                                    dictData2 = lines[index + row + 1].split()
                                    oracle_value_pair_dic[key] = " ".join(dictData2[1:])

                                    if group_param in oracle_result:
                                        oracle_result[group_param].append(oracle_value_pair_dic)
                                    else:
                                        oracle_result[group_param] = []
                                        oracle_result[group_param].append(oracle_value_pair_dic)
                                    break

                                # End of MO
                            if dictData[0][0] == "=":

                                # 2020-12-04 - Since Developer not push to Mongo, comment below line to reserve memory
                                # if group_param in mongo_result:
                                #     mongo_result[group_param].append(mongo_value_pair_dic)
                                # else:
                                #     mongo_result[group_param] = []
                                #     mongo_result[group_param].append(mongo_value_pair_dic)

                                if group_param in oracle_result:
                                    oracle_result[group_param].append(oracle_value_pair_dic)
                                else:
                                    oracle_result[group_param] = []
                                    oracle_result[group_param].append(oracle_value_pair_dic)
                                break

                            if dictData[0] == "Struct":

                                size_number = int(dictData[3])

                                row = row + 1
                                for i in range(size_number):
                                    struct_dict = lines[index + row].split()
                                    tail_name = struct_dict[1].split('.')
                                    obj_key = tail_name[1]
                                    key = naming_helper.rule_column_name(dictData[1] + "_" + obj_key)

                                    # 2020-12-04 - Since Developer not push to Mongo, comment below line to reserve memory
                                    # mongo_value_pair_dic[key] = " ".join(struct_dict[3:])

                                    if key in param_collection:
                                        oracle_value_pair_dic[key] = " ".join(struct_dict[3:])

                                    row = row + 1

                                continue

                            if (dictData[0] == ">>>" and 'Struct[' in dictData[1]):
                                size_number = int(dictData[3])

                                previous_row = row - 1

                                row = row + 1
                                for i in range(size_number):
                                    struct_dict = lines[index + row].split()
                                    keyname = lines[index + previous_row].split()
                                    if len(keyname) > 0:
                                        key_match = re.match("(\w+)\[\d+\].*",keyname[0])                                    
                                        if key_match:
                                            keyname = str(key_match.group(1)).strip()
                                        else:
                                            keyname = keyname[0]
                                    else:
                                        keyname = dictData[1]
                                    tail_name = struct_dict[1].split('.')
                                    obj_key = tail_name[1]
                                    key = naming_helper.rule_column_name(keyname + "_" + obj_key)

                                    # 2020-12-04 - Since Developer not push to Mongo, comment below line to reserve memory
                                    # mongo_value_pair_dic[key] = " ".join(struct_dict[3:])

                                    if key in param_collection:
                                        oracle_value_pair_dic[key] = " ".join(struct_dict[3:])

                                    row = row + 1

                                continue

                            if dictData[0][:11] == "reservedBy[":
                                size_number = dictData[0].split('[')
                                size_number = size_number[1].split(']')
                                size_number = int(size_number[0])

                                reserved_list = []
                                row = row + 1

                                for i in range(size_number):
                                    reserved_dict = lines[index + row].split()

                                    value = reserved_dict[3]
                                    reserved_list.append(value)
                                    row = row + 1

                                    if group_param == 'SECTORCARRIER' or group_param == "UTRANFREQUENCY" or group_param == 'EUTRANFREQUENCY':
                                        if "EUtranCellFDD" in value:

                                            tmp_full_dic = value.split(",")
                                            for tmp_dic in tmp_full_dic:
                                                if "EUtranCellFDD" in tmp_dic:
                                                    tmp_dic = tmp_dic.split("=")

                                                    # mongo_value_pair_dic[naming_helper.rule_column_name("reservedBy_" + "EUtranCellFDD")] = tmp_dic[1]
                                                    # oracle_value_pair_dic[naming_helper.rule_column_name("reservedBy_" + "EUtranCellFDD")] = tmp_dic[1]

                                                    # 2020-12-04 - Since Developer not push to Mongo, comment below line to reserve memory
                                                    # mongo_value_pair_dic[KEY_REFERENCE_FIELD] = tmp_dic[1]
                                                    oracle_value_pair_dic[KEY_REFERENCE_FIELD] = tmp_dic[1]

                                        elif 'EUtranCellTDD' in value:
                                            tmp_full_dic = value.split(",")
                                            for tmp_dic in tmp_full_dic:
                                                if "EUtranCellTDD" in tmp_dic:
                                                    tmp_dic = tmp_dic.split("=")
                                                    # 2020-12-04 - Since Developer not push to Mongo, comment below line to reserve memory
                                                    # mongo_value_pair_dic[KEY_REFERENCE_FIELD] = tmp_dic[1]
                                                    oracle_value_pair_dic[KEY_REFERENCE_FIELD] = tmp_dic[1]

                                    # How about group NRSectorCarrier
                                    elif group_param == 'NRSECTORCARRIER' or group_param == 'COMMONBEAMFORMING' or group_param == 'NRFREQUENCY':
                                        if "NRCellDU" in value:
                                            tmp_full_dic = value.split(",")
                                            for tmp_dic in tmp_full_dic:
                                                if "NRCellDU" in tmp_dic:
                                                    tmp_dic = tmp_dic.split("=")
                                                    # 2020-12-04 - Since Developer not push to Mongo, comment below line to reserve memory
                                                    # mongo_value_pair_dic[KEY_REFERENCE_FIELD] = tmp_dic[1]
                                                    oracle_value_pair_dic[KEY_REFERENCE_FIELD] = tmp_dic[1]

                                        elif "NRCellCU" in value:
                                            tmp_full_dic = value.split(",")
                                            for tmp_dic in tmp_full_dic:
                                                if "NRCellCU" in tmp_dic:
                                                    tmp_dic = tmp_dic.split("=")
                                                    # 2020-12-04 - Since Developer not push to Mongo, comment below line to reserve memory
                                                    # mongo_value_pair_dic[KEY_REFERENCE_FIELD] = tmp_dic[1]
                                                    oracle_value_pair_dic[KEY_REFERENCE_FIELD] = tmp_dic[1]
                                continue

                            # 2020-12-04 - Since Developer not push to Mongo, comment below line to reserve memory
                            # mongo_value_pair_dic[dictData[0].upper()] = " ".join(dictData[1:])

                            tagTmp = (dictData[0] + "_" + qciProfilePredefinedId).upper()

                            if naming_helper.rule_column_name(dictData[0].upper()) in param_collection:
                                oracle_value_pair_dic[naming_helper.rule_column_name(dictData[0].upper())] = " ".join(dictData[1:])
                            elif naming_helper.rule_column_name(tagTmp) in param_collection:
                                oracle_value_pair_dic[naming_helper.rule_column_name(tagTmp)] = " ".join(dictData[1:])

                            elif dictData[0] == "logicalChannelGroupRef":

                                tmp = dictData[1].split("LogicalChannelGroup=")
                                tagTmp = "logicalChannelGroup_" + qciProfilePredefinedId
                                if naming_helper.rule_column_name(tagTmp) in param_collection:
                                    oracle_value_pair_dic[naming_helper.rule_column_name(tagTmp.upper())] = tmp[1]

                            row += 1

    # sw_result = {}
    # dic = dict.fromkeys(sw_column, '')
    # dic["NAME"] = sw_name
    # dic["FAMILYTYPE"] = sw_familytype
    # dic["SWVERSION"] = sw_version
    #
    # dic["NETYPENAME"] = sw_netypename
    # dic["NEFUNCTION"] = sw_nefunction
    #
    # dic["FILENAME"] = filename
    # dic["REFERENCE_FIELD"] = sw_name
    #
    # sw_key = "SW_" + ERICSSON_TABLE_PREFIX + "_" + frequency_type
    #
    # sw_result[sw_key] = []
    # sw_result[sw_key].append(dic)
    #
    # ran_baseline_oracle.push(oracle_cur, sw_key, sw_result[sw_key])

    # log.i("----- Start mongo.push : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    # for group_param in mongo_result:
    #     collection_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX, frequency_type, group_param)
    #     granite_mongo.push(collection_name, mongo_result[group_param])
    elapsed_time = timer() - t
    log.i(f"----- Finish raw.process: {str(elapsed_time)}", ERICSSON_VENDOR, frequency_type)
    log.i("----- Start oracle.push : " + str(datetime.datetime.now()), ERICSSON_VENDOR, frequency_type)
    t = timer()
    for group_param in oracle_result:
        try:

            param_collection = field_mapping_dic[group_param]

            if group_param == 'QCIPROFILEPREDEFINED' or group_param == 'RNCFEATURE':
                qci = oracle_result[group_param]

                oracle_value_pair_dic = dict.fromkeys(param_collection, '')
                for item in qci:

                    for obj in item:

                        if obj == 'FILENAME' and (item[obj] is '' or item[obj] is None):
                            oracle_value_pair_dic = dict.fromkeys(param_collection, '')
                            break

                        if obj == 'MO' and (item[obj] is '' or item[obj] is None):
                            oracle_value_pair_dic = dict.fromkeys(param_collection, '')
                            break

                        if obj == 'MO' and ('QciProfilePredefined=default' in item[obj]):
                            oracle_value_pair_dic = dict.fromkeys(param_collection, '')
                            break

                        if item[obj] is not None and item[obj] is not '':
                            # print(obj)
                            oracle_value_pair_dic[obj] = item[obj]
                            # print(oracle_value_pair_dic)
                tmp = oracle_value_pair_dic["FILENAME"]
                print("=== FILENAME", tmp)

                tmpArr = oracle_value_pair_dic["MO"].split(",QciTable")
                oracle_value_pair_dic["MO"] = tmpArr[0]
                tmp2 = oracle_value_pair_dic["MO"]

                tmpArr = tmp2.split(",RncFeature")
                oracle_value_pair_dic["MO"] = tmpArr[0]
                tmp2 = oracle_value_pair_dic["MO"]

                print("=== MO", tmp2)

                if oracle_value_pair_dic["FILENAME"] is not None and oracle_value_pair_dic["FILENAME"] is not '':
                    oracle_result[group_param] = []
                    oracle_result[group_param].append(oracle_value_pair_dic)

            collection_name = naming_helper.get_table_name(ERICSSON_TABLE_PREFIX, frequency_type, group_param)
            ran_baseline_oracle.push(oracle_cur, collection_name, oracle_result[group_param])
            # oracle_con.commit()
        except:
            traceback.print_exc()
            pass    

    oracle_con.commit()
    mongo_result.clear()
    oracle_result.clear()
    elapsed_time = timer() - t
    log.i(f"----- Finish oracle.push: {str(elapsed_time)}", ERICSSON_VENDOR, frequency_type)
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
    is_mo = False
    productNumber = ""
    productRevision = ""
    while True:
        dictData = lines[index + row].split()

        # End of MO
        if dictData[0][0] == "=":
            return ""

        key = dictData[0].upper()
        value = " ".join(dictData[1:])

        if key.upper() == "backupName".upper() and value.startswith("Final_backup"):
            is_mo = True

        if is_mo:
            matches = re.search(REGEX_PRODUCTNUMBER, value)

            if matches:
                productNumber = matches.group(1)

            matches = re.search(REGEX_PRODUCTREVISION, value)

            if matches:
                productRevision = matches.group(1)

        if productNumber != "" and productRevision != "":
            return productNumber + "_" + productRevision

        row += 1


def find_swversion_4g_tmp(lines, index):
    row = 2

    productNumber = ""
    productRevision = ""
    while True:
        dictData = lines[index + row].split()

        # End of MO
        if dictData[0][0] == "=":
            return ""

        value = " ".join(dictData[1:])

        matches = re.search(REGEX_PRODUCTNUMBER, value)

        if matches:
            productNumber = matches.group(1)

        matches = re.search(REGEX_PRODUCTREVISION, value)

        if matches:
            productRevision = matches.group(1)

        if productNumber != "" and productRevision != "":
            return productNumber + "_" + productRevision

        row += 1


def find_feature_version_3g_4g(lines, index):
    row = 2
    is_mo = False
    key_id = ""
    featurestate = ""
    description = ""
    licensestate = ""
    servicestate = ""
    while True:
        dictData = lines[index + row].split()
        row += 1

        # End of MO
        if dictData[0][0] == "=":
            return key_id, featurestate, description, servicestate, licensestate

        key = dictData[0].upper()
        value = " ".join(dictData[1:])

        if key.upper() == 'featureState'.upper():
            featurestate = value
            continue

        if key.upper() == 'description'.upper():
            description = value
            continue

        if key.upper() == 'RncFeatureId'.upper():
            description = value
            continue

        if key.upper() == 'keyid'.upper():
            key_id = value
            continue
        
        #CR-2020 - Added serviceState and licenseState
        if key.upper() == 'licenseState'.upper():
            licensestate = value
            continue

        if key.upper() == 'serviceState'.upper():
            servicestate = value
            continue

        if featurestate != '' and key_id != '' and description != '' and not servicestate and not licensestate:
            return key_id, featurestate, description, servicestate, licensestate


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


def find_netypename_baseband(lines, index):
    row = 2
    while True:
        dictData = lines[index + row].split()

        # End of MO
        if dictData[0][0] == "=":
            return ""

        value = " ".join(dictData[1:])

        if "productName" in value:
            dictData = value.split("=")
            value = " ".join(dictData[1:])

            if value.startswith("Baseband") or value.startswith("DUS"):
                return "RBS6601"

            return value.strip()

        row += 1


def find_swversion_2g(lines, index):
    row = 2
    is_mo = False
    productNumber = ""
    productName = ""
    productStatus = ""

    while True:
        dictData = lines[index + row].split()

        # End of MO
        if dictData[0][0] == "=":
            return productNumber, productName, productStatus

        key = dictData[0].upper()
        value = " ".join(dictData[1:])

        if key.upper() == "backupName".upper() and value.startswith("Final_backup"):
            is_mo = True

        if is_mo:
            matches = re.search(REGEX_PRODUCTNUMBER, value)
            if matches:
                productNumber = matches.group(1)

            matches = re.search(REGEX_PRODUCTNAME, value)
            if matches:
                productName = matches.group(1)

            if key == "STATUS":
                productStatus = value

        if productNumber != "" and productName != "" and productStatus != "":
            return productNumber, productName, productStatus

        row += 1


def find_swversion_2g_tmp(lines, index):
    row = 2
    is_mo = False
    productNumber = ""
    productName = ""
    productStatus = ""

    while True:
        dictData = lines[index + row].split()

        # End of MO
        if dictData[0][0] == "=":
            return productNumber, productName, productStatus

        key = dictData[0].upper()
        value = " ".join(dictData[1:])

        matches = re.search(REGEX_PRODUCTNUMBER, value)
        if matches:
            productNumber = matches.group(1)

        matches = re.search(REGEX_PRODUCTNAME, value)
        if matches:
            productName = matches.group(1)

        if key == "STATUS":
            productStatus = value

        if productNumber != "" and productName != "" and productStatus != "":
            return productNumber, productName, productStatus

        row += 1


def find_swversion_2g_bts(lines, index):
    row = 2

    bscNodeIdentity = ""
    bscTgIdentity = ""
    gsmSectorId = ""

    while True:
        dictData = lines[index + row].split()

        # End of MO
        if dictData[0][0] == "=":
            return bscNodeIdentity, bscTgIdentity, gsmSectorId

        key = dictData[0].upper()
        value = " ".join(dictData[1:])

        if key == "BSCNODEIDENTITY":
            bscNodeIdentity = value

        if key == "BSCNODEIDENTITY":
            bscNodeIdentity = value

        if key == "BSCTGIDENTITY":
            bscTgIdentity = value

        if key == "GSMSECTORID":
            gsmSectorId = value

        if bscNodeIdentity != "" and bscTgIdentity != "" and gsmSectorId != "":
            return bscNodeIdentity, bscTgIdentity, gsmSectorId

        row += 1


def find_swversion_2g_name(lines, index):
    row = 2

    while True:
        dictData = lines[index + row].split()

        # End of MO
        if dictData[0][0] == "=":
            return ""


        key = dictData[0].upper()
        value = " ".join(dictData[1:])

        if key == "MANAGEDELEMENTID":
            return value

        row += 1
