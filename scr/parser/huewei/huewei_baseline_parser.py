# encoding: utf-8
import datetime
import multiprocessing as mp
import traceback
from typing import Dict
from lxml import etree
from lxml.etree import XPathEvalError

import cx_Oracle
import pymongo
from lxml import ElementInclude

import log
from environment import *
from environment import HUAWEI_TABLE_PREFIX, BASELINE_TABLE_PREFIX
from scr.dao import ran_baseline_oracle
from scr.helper import naming_helper
from scr.helper.naming_helper import PARSING_TABLE_STATEMENT, PREPARING_TABLE_STATEMENT, PARSING_FILE_STATEMENT

xml_namespaces = {'schemaLocation': 'http://www.huawei.com/specs/huawei_wl_bulkcm_xml_baseline_syn_1.0.0',
                  'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
                  'spec': 'http://www.huawei.com/specs/huawei_wl_bulkcm_xml_baseline_syn_1.0.0'}

VALUE_PAIR_SEPARATOR = ';'
PARAM_VALUE_ASSIGNER = ':'

KEY_BSC_LEVEL = "BSC Level"
KEY_CELL_LEVEL = "CELL Level"
KEY_NODEB_LEVEL = "NODEB level"
KEY_RNC_LEVEL = "RNC level"

KEY_SYSOBJECTID = "SYSOBJECTID"
KEY_CELLID = "CELLID"
KEY_NODEBID = "NODEBID"
KEY_CI = "CI"
KEY_SRCLTENCELLID = "SRCLTENCELLID"
KEY_TRXID = "TRXID"

KEY_CELLNAME = "CELLNAME"
KEY_NENAME = "NENAME"
KEY_LOCALCELLID = "LocalCellId"

KEY_REFERENCE_FIELD = "REFERENCE_FIELD"
KEY_MO_FIELD = "MO"
FILENAME_FIELD = "FILENAME"
LV_FIELD = "LV"

SYSOBJECTID = ""

# 5G KeyID
KEY_NR_CELLID = "NrCellId"
KEY_NR_DU_CELLID = "NrDuCellId"

encodings = ['utf-8', 'windows-1250', 'windows-1252']

sw_2g_column = [
    "SYSOBJECTID",
    "SYSDESC",
    "SWVERSION",
    "FILENAME",
    "REFERENCE_FIELD",
    "MO"
]

sw_4g_column = [
    "NENAME",
    "PRODUCTVERSION",
    "SWVERSION",
    "REFERENCE_FIELD",
    "MO",
    "NEFUNCTION",
    "FILENAME"
]

KEY_TABLE = "{0}_{1}_{2}"


def close_connection(connection, cur):
    cur.close()
    connection.close()


def open_connection():
    dsn_tns = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, ORACLE_SID)
    connection = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, dsn_tns)
    cur = connection.cursor()
    return connection, cur


def prepare_oracle_table_2g(oracle_con, oracle_cur, frequency_type, field_mapping_dic, base_mapping_dic, drop_param=True, baseline_label_dic = {}):
    log.i("----- ", HUAWEI_VENDOR, frequency_type)
    log.i("----------------------------------------------------------", HUAWEI_VENDOR, frequency_type)
    log.i(PREPARING_TABLE_STATEMENT + " : " + HUAWEI_VENDOR + " : " + frequency_type, HUAWEI_VENDOR, frequency_type)

    for group_param in field_mapping_dic:
        table_name = naming_helper.get_table_name(BASELINE_TABLE_PREFIX.format(HUAWEI_TABLE_PREFIX), frequency_type, group_param)
        column_collection = field_mapping_dic[group_param]

        if (HUAWEI_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, table_name)
            ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_dic[group_param])
            ran_baseline_oracle.push(oracle_cur, table_name, baseline_label_dic[group_param])

    if drop_param:
        if (HUAWEI_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, "SW_" + HUAWEI_TABLE_PREFIX + "_" + frequency_type)
            ran_baseline_oracle.create_table(oracle_cur, "SW_" + HUAWEI_TABLE_PREFIX + "_" + frequency_type, sw_2g_column)

        for group_param in field_mapping_dic:
            table_name = naming_helper.get_table_name(HUAWEI_TABLE_PREFIX, frequency_type, group_param)
            column_collection = field_mapping_dic[group_param]

            if (HUAWEI_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
                ran_baseline_oracle.drop(oracle_cur, table_name)
                ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

    CREATED_TABLE[HUAWEI_TABLE_PREFIX + "_" + frequency_type] = []

    oracle_con.commit()
    return


def prepare_oracle_table_3g(oracle_con, oracle_cur, frequency_type, field_mapping_dic, base_mapping_850bma_dic, base_mapping_850upc_dic, base_mapping_2100_dic, drop_param=True, base_mapping_label_dic = {}):
    log.i("----- ", HUAWEI_VENDOR, frequency_type)
    log.i("----------------------------------------------------------", HUAWEI_VENDOR, frequency_type)
    log.i(PREPARING_TABLE_STATEMENT + " : " + HUAWEI_VENDOR + " : " + frequency_type, HUAWEI_VENDOR, frequency_type)

    for group_param in field_mapping_dic:
        table_name = naming_helper.get_table_name(BASELINE_TABLE_PREFIX.format(HUAWEI_TABLE_PREFIX), frequency_type, group_param)
        column_collection = field_mapping_dic[group_param]

        if (HUAWEI_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, table_name)
            ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_850bma_dic[group_param])
            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_850upc_dic[group_param])
            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_2100_dic[group_param])

            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_label_dic[group_param])

    if drop_param:

        if (HUAWEI_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, "SW_" + HUAWEI_TABLE_PREFIX + "_" + frequency_type)
            ran_baseline_oracle.create_table(oracle_cur, "SW_" + HUAWEI_TABLE_PREFIX + "_" + frequency_type, sw_2g_column)

        for group_param in field_mapping_dic:
            table_name = naming_helper.get_table_name(HUAWEI_TABLE_PREFIX, frequency_type, group_param)
            column_collection = field_mapping_dic[group_param]

            if (HUAWEI_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
                ran_baseline_oracle.drop(oracle_cur, table_name)
                ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

    CREATED_TABLE[HUAWEI_TABLE_PREFIX + "_" + frequency_type] = []

    oracle_con.commit()
    return


def prepare_oracle_table_4g(oracle_con, oracle_cur, frequency_type, field_mapping_dic, base_mapping_900_dic, base_mapping_1800_dic, base_mapping_2100_dic, base_mapping_2600_dic, drop_param=True, base_mapping_label_dic = {}):
    log.i(PREPARING_TABLE_STATEMENT + " : " + HUAWEI_VENDOR + " : " + frequency_type, HUAWEI_VENDOR, frequency_type)

    for group_param in field_mapping_dic:
        table_name = naming_helper.get_table_name(BASELINE_TABLE_PREFIX.format(HUAWEI_TABLE_PREFIX), frequency_type, group_param)
        column_collection = field_mapping_dic[group_param]

        if (HUAWEI_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, table_name)
            ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_900_dic[group_param])
            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_1800_dic[group_param])
            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_2100_dic[group_param])
            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_2600_dic[group_param])
            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_label_dic[group_param])

    if drop_param:
        if (HUAWEI_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, "SW_" + HUAWEI_TABLE_PREFIX + "_" + frequency_type)
            ran_baseline_oracle.create_table(oracle_cur, "SW_" + HUAWEI_TABLE_PREFIX + "_" + frequency_type, sw_4g_column)

        for group_param in field_mapping_dic:
            table_name = naming_helper.get_table_name(HUAWEI_TABLE_PREFIX, frequency_type, group_param)
            column_collection = field_mapping_dic[group_param]

            if (HUAWEI_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
                ran_baseline_oracle.drop(oracle_cur, table_name)
                ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

    CREATED_TABLE[HUAWEI_TABLE_PREFIX + "_" + frequency_type] = []
    oracle_con.commit()

    return

def prepare_oracle_table_5g(oracle_con, oracle_cur, frequency_type, field_mapping_dic, base_mapping_2600_dic, drop_param=True, base_mapping_label_dic = {}):
    log.i(PREPARING_TABLE_STATEMENT + " : " + HUAWEI_VENDOR + " : " + frequency_type, HUAWEI_VENDOR, frequency_type)

    for group_param in field_mapping_dic:
        table_name = naming_helper.get_table_name(BASELINE_TABLE_PREFIX.format(HUAWEI_TABLE_PREFIX), frequency_type, group_param)
        column_collection = field_mapping_dic[group_param]

        if (HUAWEI_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, table_name)
            ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_2600_dic[group_param])
            ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_label_dic[group_param])

    if drop_param:
        if (HUAWEI_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
            ran_baseline_oracle.drop(oracle_cur, "SW_" + HUAWEI_TABLE_PREFIX + "_" + frequency_type)
            ran_baseline_oracle.create_table(oracle_cur, "SW_" + HUAWEI_TABLE_PREFIX + "_" + frequency_type, sw_4g_column)

        for group_param in field_mapping_dic:
            table_name = naming_helper.get_table_name(HUAWEI_TABLE_PREFIX, frequency_type, group_param)
            column_collection = field_mapping_dic[group_param]

            if (HUAWEI_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
                ran_baseline_oracle.drop(oracle_cur, table_name)
                ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

    CREATED_TABLE[HUAWEI_TABLE_PREFIX + "_" + frequency_type] = []
    oracle_con.commit()

    return


def run(source, field_mapping_dic, param_cell_level_dic):
    log.i(PARSING_TABLE_STATEMENT, HUAWEI_VENDOR, "NA")

    pool = mp.Pool(processes=MAX_RUNNING_PROCESS)

    for raw_file in source.RawFileList:

        if source.FrequencyType == '2G':
            cell = get_2g_root_cell(raw_file)
            gtrx = get_2g_gtrx_cell(raw_file)
            pool.apply_async(parse_2g_3g, args=(raw_file, source.FrequencyType, field_mapping_dic, param_cell_level_dic, cell, {}, gtrx,))
        elif source.FrequencyType == '3G':
            cell = get_3g_root_cell(raw_file)
            nodeb = get_nodeb_list(raw_file)
            pool.apply_async(parse_2g_3g, args=(raw_file, source.FrequencyType, field_mapping_dic, param_cell_level_dic, cell, nodeb,))
        
        elif source.FrequencyType == '4G':
            pool.apply_async(parse_4g, args=(raw_file, source.FrequencyType, field_mapping_dic, param_cell_level_dic,))

        elif source.FrequencyType == '5G':
            pool.apply_async(parse_5g, args=(raw_file, source.FrequencyType, field_mapping_dic, param_cell_level_dic,))
        # parse_4g(raw_file, source.FrequencyType, field_mapping_dic, param_cell_level_dic)

    pool.close()
    pool.join()

    print("Done -------Pool ALL")


# For 2G, 3G
def parse_2g_3g(raw_file, frequency_type, field_mapping_dic, param_cell_level_dic, cell, nodeb={}, gtrx={}):
    log.i(PARSING_FILE_STATEMENT.format(raw_file))
    log.i("----- Start Parser : " + str(datetime.datetime.now()), HUAWEI_VENDOR, frequency_type)

    oracle_con, oracle_cur = open_connection()

    mongo_result = {}
    oracle_result = {}

    sys_object_id = get_sysobjectid(raw_file)
    sysdesc = get_sysdesc(raw_file)
    swversion = get_swversion_2g_3g(raw_file)
    filename_dic = raw_file.split("/")

    filename = filename_dic[len(filename_dic) - 1]

    log.i(">>>> Start Parser", HUAWEI_VENDOR, frequency_type)

    start_parser_time = datetime.datetime.now()

    sw_result = {}
    dic = dict.fromkeys(sw_2g_column, '')
    dic["SYSOBJECTID"] = sys_object_id
    dic["SYSDESC"] = sysdesc
    dic["SWVERSION"] = swversion
    dic["FILENAME"] = filename
    dic["REFERENCE_FIELD"] = sys_object_id
    dic["MO"] = KEY_SYSOBJECTID + "=" + sys_object_id

    sw_key = "SW_" + HUAWEI_TABLE_PREFIX + "_" + frequency_type

    sw_result[sw_key] = []
    sw_result[sw_key].append(dic)

    ran_baseline_oracle.push(oracle_cur, sw_key, sw_result[sw_key])
    oracle_con.commit()

    with open(raw_file) as f:

        encodings = ['utf-8', 'windows-1250', 'windows-1252']

        for e in encodings:
            with open(raw_file, encoding=e, errors='ignore') as f:

                try:
                    for line in f:

                        extracted_by_colon_line = line.split(':')

                        if extracted_by_colon_line.__len__() <= 1:
                            continue

                        split_line = extracted_by_colon_line[0].split(' ')

                        if split_line.__len__() <= 1:
                            continue

                        group_param = split_line[1]
                        if group_param not in field_mapping_dic:
                            continue

                        group_level = param_cell_level_dic[group_param]

                        mo = KEY_SYSOBJECTID + "=" + sys_object_id
                        reference_field = sys_object_id

                        param_collection = field_mapping_dic[group_param]

                        value_zone = extracted_by_colon_line[1].split(',')

                        mongo_value_pair_dic = {}
                        oracle_value_pair_dic = dict.fromkeys(param_collection, '')

                        for value_pair in value_zone:
                            extracted_value_pair = value_pair.split('=')
                            key = str(extracted_value_pair[0]).strip()
                            value = str(extracted_value_pair[1]).strip().replace('\"', '')
                            value = naming_helper.clean_value_data(value)

                            if group_level.upper() == KEY_CELL_LEVEL.upper():

                                if group_param.upper() == "GEXTLTECELL" or group_param.upper() == "GEXT2GCELL":
                                    if key.upper() == KEY_CI.upper():
                                        mo = mo + "," + KEY_CI + "=" + value

                                        if value in cell:
                                            reference_field = cell[value]
                                        else:
                                            reference_field = ""

                                elif group_param.upper() == "GLTENCELL":
                                    if key.upper() == KEY_SRCLTENCELLID.upper():

                                        mo = mo + "," + KEY_SRCLTENCELLID + "=" + value

                                        if value in cell:
                                            reference_field = cell[value]
                                        else:
                                            reference_field = ""

                                elif group_param.upper() == "GTRXDEV":
                                    if key.upper() == KEY_TRXID.upper():

                                        mo = mo + "," + KEY_CELLID + "=" + gtrx[value]

                                        if gtrx[value] in cell:
                                            reference_field = cell[gtrx[value]]
                                        else:
                                            reference_field = ""

                                else:

                                    if key.upper() == KEY_CELLID.upper():
                                        mo = mo + "," + KEY_CELLID + "=" + value

                                        if value in cell:
                                            reference_field = cell[value]
                                        else:
                                            reference_field = ""

                            elif group_level.upper() == KEY_NODEB_LEVEL.upper():
                                if key.upper() == KEY_NODEBID.upper():
                                    mo = mo + "," + KEY_NODEBID + "=" + value

                                    if value in nodeb:
                                        reference_field = nodeb[value]
                                    else:
                                        reference_field = ""

                            elif group_level.upper() == KEY_BSC_LEVEL.upper():

                                if key.upper() == "CNNODEIDX":
                                    mo = mo + ",CNNODEIDX=" + value

                                reference_field = sys_object_id

                            elif group_level.upper() == KEY_RNC_LEVEL.upper():
                                reference_field = sys_object_id

                            else:
                                mo = "n/a lv : " + group_level
                                reference_field = "n/a lv : " + group_level

                            mongo_value_pair_dic[key] = value

                            if key in param_collection:
                                oracle_value_pair_dic[key] = value

                            if '&' in value:
                                comment_dic = value.split("&")

                                for comment in comment_dic:
                                    if '-' in comment:
                                        comments = comment.split("-")
                                        comments_key = str(comments[0]).strip()
                                        comments_value = str(comments[1]).strip().replace('\"', '')
                                        comments_value = naming_helper.clean_value_data(comments_value)

                                        comments_key = naming_helper.get_huawei_comment_column_name("C", key, comments_key)

                                        mongo_value_pair_dic[comments_key] = comments_value

                                        if comments_key in param_collection:
                                            oracle_value_pair_dic[comments_key] = comments_value

                        if KEY_TABLE.format(HUAWEI_TABLE_PREFIX, frequency_type, group_param) not in COUNT_DATA:
                            COUNT_DATA[KEY_TABLE.format(HUAWEI_TABLE_PREFIX, frequency_type, group_param)] = 0
                        COUNT_DATA[KEY_TABLE.format(HUAWEI_TABLE_PREFIX, frequency_type, group_param)] = COUNT_DATA[KEY_TABLE.format(HUAWEI_TABLE_PREFIX, frequency_type, group_param)] + 1

                        mongo_value_pair_dic[KEY_MO_FIELD] = mo
                        mongo_value_pair_dic[KEY_REFERENCE_FIELD] = reference_field
                        mongo_value_pair_dic[FILENAME_FIELD] = filename
                        mongo_value_pair_dic[LV_FIELD] = group_level

                        oracle_value_pair_dic[KEY_MO_FIELD] = mo
                        oracle_value_pair_dic[KEY_REFERENCE_FIELD] = reference_field
                        oracle_value_pair_dic[FILENAME_FIELD] = filename
                        oracle_value_pair_dic[LV_FIELD] = group_level

                        if group_param in mongo_result:
                            mongo_result[group_param].append(mongo_value_pair_dic)
                        else:
                            mongo_result[group_param] = []
                            mongo_result[group_param].append(mongo_value_pair_dic)

                        if group_param in oracle_result:
                            oracle_result[group_param].append(oracle_value_pair_dic)
                        else:
                            oracle_result[group_param] = []
                            oracle_result[group_param].append(oracle_value_pair_dic)
                except UnicodeDecodeError:
                    print('got unicode error with %s , trying different encoding' % e)

                else:
                    print('opening the file with encoding:  %s ' % e)
                    break


    parser_time = datetime.datetime.now()
    parser_time = str(parser_time - start_parser_time)
    log.i("--- Parser itme : " + str(parser_time), HUAWEI_VENDOR, frequency_type)

    my_client = pymongo.MongoClient(MONGO_HOST)
    my_db = my_client[MONGO_NAME]

    start_db_time = datetime.datetime.now()
    # log.i("--- Start mongo.push", HUAWEI_VENDOR, frequency_type)
    # for group_param in mongo_result:
    #     collection_name = naming_helper.get_table_name(HUAWEI_TABLE_PREFIX, frequency_type, group_param)
        # granite_mongo.push(collection_name, mongo_result[group_param])
        # my_collection = my_db[collection_name]
        # my_collection.insert_many(mongo_result[group_param])
    # my_client.close()
    # log.i("----- Done mongo time : " + str(datetime.datetime.now() - start_db_time), HUAWEI_VENDOR, frequency_type)

    start_db_time = datetime.datetime.now()
    log.i("--- Start oracle.push", HUAWEI_VENDOR, frequency_type)
    for group_param in oracle_result:
        try:
            collection_name = naming_helper.get_table_name(HUAWEI_TABLE_PREFIX, frequency_type, group_param)
            ran_baseline_oracle.push(oracle_cur, collection_name, oracle_result[group_param])
        except:
            traceback.print_exc()

    oracle_con.commit()
    close_connection(oracle_con, oracle_cur)

    log.i("----- Done oracle time : " + str(datetime.datetime.now() - start_db_time), HUAWEI_VENDOR, frequency_type)

    mongo_result.clear()
    oracle_result.clear()

    log.i("--- Done ", HUAWEI_VENDOR, frequency_type)
    log.i("<<<< Time : " + str(datetime.datetime.now() - start_parser_time), HUAWEI_VENDOR, frequency_type)

    log.i("    ", HUAWEI_VENDOR, frequency_type)
    log.i("    ", HUAWEI_VENDOR, frequency_type)


def parse_4g(raw_file, frequency_type, field_mapping_dic, param_cell_level_dic):
    log.i(PARSING_FILE_STATEMENT.format(raw_file))

    oracle_con, oracle_cur = open_connection()

    log.i("----- Start Parser HW 4G", HUAWEI_VENDOR, frequency_type)

    tree = ElementInclude.default_loader(raw_file, 'xml')

    nename = get_nename(tree)
    productversion = get_productversion(tree)
    nefunction = get_nefunction(tree, nename)
    swversion = get_swversion_4g(tree)

    cell = get_4g_root_cell(tree)

    filename_dic = raw_file.split("/")

    filename = filename_dic[len(filename_dic) - 1]

    mongo_result = {}
    oracle_result = {}
    sw_result = {}

    dic = dict.fromkeys(sw_4g_column, '')
    dic["NENAME"] = nename
    dic["PRODUCTVERSION"] = productversion
    dic["SWVERSION"] = swversion
    dic["REFERENCE_FIELD"] = nename
    dic["MO"] = KEY_NENAME + "=" + nename
    dic["NEFUNCTION"] = nefunction
    dic["FILENAME"] = filename

    sw_key = "SW_" + HUAWEI_TABLE_PREFIX + "_" + frequency_type

    sw_result[sw_key] = []
    sw_result[sw_key].append(dic)

    ran_baseline_oracle.push(oracle_cur, sw_key, sw_result[sw_key])
    oracle_con.commit()

    xpath = './/spec:syndata'

    sectoreqmref = get_sectoreqmref(tree)

    class_node_collections = tree.xpath(xpath, namespaces=xml_namespaces)

    for class_node_collection in class_node_collections:

        for class_node in class_node_collection:
            for group_node in class_node:
                group_param = remove_xml_descriptor(group_node.tag).upper()

                if group_param not in mongo_result:
                    mongo_result[group_param] = []
                    oracle_result[group_param] = []

                if group_param not in field_mapping_dic:
                    continue

                group_param = group_param
                group_level = param_cell_level_dic[group_param]

                param_collection = field_mapping_dic[group_param]

                mongo_value_pair_dic = {}
                oracle_value_pair_dic = dict.fromkeys(param_collection, '')

                mo = KEY_NENAME + "=" + nename
                reference_field = ""

                last_param = ""
                for attribute_node in group_node:
                    for attribute in attribute_node:

                        # no comment
                        try:
                            key = remove_xml_descriptor(attribute.tag).upper()
                            value = str(attribute.text).strip()

                            value = naming_helper.clean_value_data(value)
                            last_param = key

                            if group_level.upper() == KEY_CELL_LEVEL.upper():

                                if group_param == 'UTRANEXTERNALCELL' or group_param == 'EUTRANEXTERNALCELL':

                                    if key.upper() == KEY_LOCALCELLID.upper() and value in cell:
                                        mo = mo + "," + KEY_LOCALCELLID + "=" + value
                                        reference_field = cell[value]

                                    elif key.upper() == KEY_CELLID.upper() and value in cell:
                                        mo = mo + "," + KEY_CELLID + "=" + value
                                        reference_field = cell[value]

                                else:
                                    if value in cell:

                                        if key.upper() == KEY_LOCALCELLID.upper():
                                            mo = mo + "," + KEY_LOCALCELLID + "=" + value

                                            reference_field = cell[value]

                                        elif key.upper() == KEY_CELLID.upper():
                                            mo = mo + "," + KEY_CELLID + "=" + value

                                            reference_field = cell[value]

                            elif group_level.upper() == KEY_NODEB_LEVEL.upper():

                                reference_field = nename

                                if value in cell:
                                    reference_field = cell[value]

                            else:
                                reference_field = nename

                            if key.upper() == 'SECTOREQMREF':

                                tmp_value = ""
                                key_element = ""
                                sectoreqm_value = ""

                                for elements in attribute:
                                    for element in elements:
                                        key_element = remove_xml_descriptor(element.tag).upper()
                                        value_element = str(element.text).strip()

                                        if tmp_value == "":
                                            tmp_value = value_element
                                            sectoreqm_value = sectoreqmref[value_element]
                                        else:
                                            tmp_value = tmp_value + "|" + value_element
                                            sectoreqm_value = sectoreqm_value + "|" + sectoreqmref[value_element]

                                value = tmp_value
                                key = "SECTOREQMID"

                                mongo_value_pair_dic["SECTORID"] = sectoreqm_value
                                oracle_value_pair_dic["SECTORID"] = sectoreqm_value

                            mongo_value_pair_dic[key.upper()] = value

                            if key.upper() in param_collection:
                                oracle_value_pair_dic[key.upper()] = value

                        # comment logic
                        except:

                            # Check Multi comments
                            if VALUE_PAIR_SEPARATOR in attribute.text:
                                extracted_value_collection = attribute.text.split(VALUE_PAIR_SEPARATOR)

                                for value_pair in extracted_value_collection:

                                    comment_key, comment_value = param_value_pair_splitter(value_pair)
                                    comment_key = naming_helper.get_huawei_comment_column_name("C", last_param, comment_key)
                                    comment_value = naming_helper.clean_value_data(comment_value)

                                    mongo_value_pair_dic[comment_key] = comment_value

                                    if comment_key in param_collection:
                                        oracle_value_pair_dic[comment_key] = comment_value

                            # Check comment key-value
                            elif PARAM_VALUE_ASSIGNER in attribute.text:

                                comment_key, comment_value = param_value_pair_splitter(attribute.text)
                                comment_key = naming_helper.get_huawei_comment_column_name("C", last_param, comment_key)

                                comment_value = naming_helper.clean_value_data(comment_value)

                                mongo_value_pair_dic[comment_key] = comment_value

                                if comment_key in param_collection:
                                    oracle_value_pair_dic[comment_key] = comment_value

                            # Comment single value
                            else:

                                comment_key = naming_helper.get_huawei_comment_column_name("C", last_param, "")
                                comment_value = str(attribute.text).strip()
                                comment_value = naming_helper.clean_value_data(comment_value)

                                mongo_value_pair_dic[comment_key] = comment_value
                                if comment_key in param_collection:
                                    oracle_value_pair_dic[comment_key] = comment_value

                if KEY_TABLE.format(HUAWEI_TABLE_PREFIX, frequency_type, group_param) not in COUNT_DATA:
                    COUNT_DATA[KEY_TABLE.format(HUAWEI_TABLE_PREFIX, frequency_type, group_param)] = 0
                COUNT_DATA[KEY_TABLE.format(HUAWEI_TABLE_PREFIX, frequency_type, group_param)] = COUNT_DATA[KEY_TABLE.format(HUAWEI_TABLE_PREFIX, frequency_type, group_param)] + 1

                mongo_value_pair_dic[KEY_MO_FIELD] = mo
                mongo_value_pair_dic[KEY_REFERENCE_FIELD] = reference_field
                mongo_value_pair_dic[FILENAME_FIELD] = filename
                mongo_value_pair_dic[LV_FIELD] = group_level

                oracle_value_pair_dic[KEY_MO_FIELD] = mo
                oracle_value_pair_dic[KEY_REFERENCE_FIELD] = reference_field
                oracle_value_pair_dic[FILENAME_FIELD] = filename
                oracle_value_pair_dic[LV_FIELD] = group_level

                mongo_result[group_param].append(mongo_value_pair_dic)
                oracle_result[group_param].append(oracle_value_pair_dic)

                collection_name = naming_helper.get_table_name(HUAWEI_TABLE_PREFIX, frequency_type, group_param)
                # granite_mongo.push(collection_name, mongo_result[group_param])
                ran_baseline_oracle.push(oracle_cur, collection_name, oracle_result[group_param])

                mongo_result.clear()
                oracle_result.clear()

    oracle_con.commit()

    close_connection(oracle_con, oracle_cur)

    log.i("Done :::: " + filename + " ::::::::", HUAWEI_VENDOR, frequency_type)
    log.i("----- Parser done ----", HUAWEI_VENDOR, frequency_type)

def parse_5g(raw_file, frequency_type, field_mapping_dic, param_cell_level_dic):
    log.i(PARSING_FILE_STATEMENT.format(raw_file))

    oracle_con, oracle_cur = open_connection()

    log.i("----- Start Parser HW 5G", HUAWEI_VENDOR, frequency_type)

    parser = etree.XMLParser(recover=True, encoding='utf-8')
    with open(raw_file, encoding="utf-8", errors='ignore') as xml_file:
        # Create a parser
        tree = etree.parse(xml_file, parser=parser)
        # tree = ElementInclude.default_loader(raw_file, 'xml')

        nename = get_gnodeB(tree)
        productversion = get_productversion(tree)
        nefunction = get_nefunction(tree, nename)
        swversion = get_swversion_4g(tree)

        filename_dic = raw_file.split("/")

        filename = filename_dic[len(filename_dic) - 1]

        mongo_result = {}
        oracle_result = {}
        sw_result = {}

        dic = dict.fromkeys(sw_4g_column, '')
        dic["NENAME"] = nename
        dic["PRODUCTVERSION"] = productversion
        dic["SWVERSION"] = swversion
        dic["REFERENCE_FIELD"] = nename
        dic["MO"] = KEY_NENAME + "=" + nename
        dic["NEFUNCTION"] = nefunction
        dic["FILENAME"] = filename

        sw_key = "SW_" + HUAWEI_TABLE_PREFIX + "_" + frequency_type

        sw_result[sw_key] = []
        sw_result[sw_key].append(dic)

        ran_baseline_oracle.push(oracle_cur, sw_key, sw_result[sw_key])
        oracle_con.commit()

        xpath = './/spec:syndata[@FunctionType="gNodeBFunction"]'

        # sectoreqmref = get_sectoreqmref(tree)

        class_node_collections = tree.xpath(xpath, namespaces=xml_namespaces)

        for class_node_collection in class_node_collections:

            # Get GNodeB
            gnodeB = get_gnodeB_xpath(class_node_collection)
            cells = get_5g_cell_xpath(tree)

            # Get Cell Data
            for class_node in class_node_collection:
                for group_node in class_node:
                    group_param = remove_xml_descriptor(group_node.tag).upper()

                    if group_param not in mongo_result:
                        mongo_result[group_param] = []
                        oracle_result[group_param] = []

                    if group_param not in field_mapping_dic:
                        continue

                    group_param = group_param
                    group_level = param_cell_level_dic[group_param]

                    param_collection = field_mapping_dic[group_param]

                    mongo_value_pair_dic = {}
                    oracle_value_pair_dic = dict.fromkeys(param_collection, '')

                    mo = KEY_NENAME + "=" + nename
                    reference_field = ""

                    last_param = ""
                    for attribute_node in group_node:
                        for attribute in attribute_node:

                            # no comment
                            try:
                                key = remove_xml_descriptor(attribute.tag).upper()
                                value = str(attribute.text).strip()

                                value = naming_helper.clean_value_data(value)
                                last_param = key

                                if group_level.upper() == KEY_CELL_LEVEL.upper():

                                    if value in cells:

                                        if key.upper() == KEY_NR_CELLID.upper():
                                            mo = mo + "," + KEY_NR_CELLID + "=" + value

                                            reference_field = cells[value]

                                        elif key.upper() == KEY_NR_DU_CELLID.upper():
                                            mo = mo + "," + KEY_NR_DU_CELLID + "=" + value

                                            reference_field = cells[value]

                                elif group_level.upper() == KEY_NODEB_LEVEL.upper():
                                    reference_field = gnodeB
                                else:
                                    reference_field = nename

                                # if key.upper() == 'SECTOREQMREF':

                                #     tmp_value = ""
                                #     key_element = ""
                                #     sectoreqm_value = ""

                                #     for elements in attribute:
                                #         for element in elements:
                                #             key_element = remove_xml_descriptor(element.tag).upper()
                                #             value_element = str(element.text).strip()

                                #             if tmp_value == "":
                                #                 tmp_value = value_element
                                #                 sectoreqm_value = sectoreqmref[value_element]
                                #             else:
                                #                 tmp_value = tmp_value + "|" + value_element
                                #                 sectoreqm_value = sectoreqm_value + "|" + sectoreqmref[value_element]

                                #     value = tmp_value
                                #     key = "SECTOREQMID"

                                #     mongo_value_pair_dic["SECTORID"] = sectoreqm_value
                                #     oracle_value_pair_dic["SECTORID"] = sectoreqm_value

                                mongo_value_pair_dic[key.upper()] = value

                                if key.upper() in param_collection:
                                    oracle_value_pair_dic[key.upper()] = value

                            # comment logic
                            except:

                                # Check Multi comments
                                if VALUE_PAIR_SEPARATOR in attribute.text:
                                    extracted_value_collection = attribute.text.split(VALUE_PAIR_SEPARATOR)

                                    for value_pair in extracted_value_collection:

                                        comment_key, comment_value = param_value_pair_splitter(value_pair)
                                        comment_key = naming_helper.get_huawei_comment_column_name("C", last_param, comment_key)
                                        comment_value = naming_helper.clean_value_data(comment_value)

                                        mongo_value_pair_dic[comment_key] = comment_value

                                        if comment_key in param_collection:
                                            oracle_value_pair_dic[comment_key] = comment_value

                                # Check comment key-value
                                elif PARAM_VALUE_ASSIGNER in attribute.text:

                                    comment_key, comment_value = param_value_pair_splitter(attribute.text)
                                    comment_key = naming_helper.get_huawei_comment_column_name("C", last_param, comment_key)

                                    comment_value = naming_helper.clean_value_data(comment_value)

                                    mongo_value_pair_dic[comment_key] = comment_value

                                    if comment_key in param_collection:
                                        oracle_value_pair_dic[comment_key] = comment_value

                                # Comment single value
                                else:

                                    comment_key = naming_helper.get_huawei_comment_column_name("C", last_param, "")
                                    comment_value = str(attribute.text).strip()
                                    comment_value = naming_helper.clean_value_data(comment_value)

                                    mongo_value_pair_dic[comment_key] = comment_value
                                    if comment_key in param_collection:
                                        oracle_value_pair_dic[comment_key] = comment_value

                    if KEY_TABLE.format(HUAWEI_TABLE_PREFIX, frequency_type, group_param) not in COUNT_DATA:
                        COUNT_DATA[KEY_TABLE.format(HUAWEI_TABLE_PREFIX, frequency_type, group_param)] = 0
                    COUNT_DATA[KEY_TABLE.format(HUAWEI_TABLE_PREFIX, frequency_type, group_param)] = COUNT_DATA[KEY_TABLE.format(HUAWEI_TABLE_PREFIX, frequency_type, group_param)] + 1

                    mongo_value_pair_dic[KEY_MO_FIELD] = mo
                    mongo_value_pair_dic[KEY_REFERENCE_FIELD] = reference_field
                    mongo_value_pair_dic[FILENAME_FIELD] = filename
                    mongo_value_pair_dic[LV_FIELD] = group_level

                    oracle_value_pair_dic[KEY_MO_FIELD] = mo
                    oracle_value_pair_dic[KEY_REFERENCE_FIELD] = reference_field
                    oracle_value_pair_dic[FILENAME_FIELD] = filename
                    oracle_value_pair_dic[LV_FIELD] = group_level

                    mongo_result[group_param].append(mongo_value_pair_dic)
                    oracle_result[group_param].append(oracle_value_pair_dic)

                    collection_name = naming_helper.get_table_name(HUAWEI_TABLE_PREFIX, frequency_type, group_param)
                    # granite_mongo.push(collection_name, mongo_result[group_param])
                    ran_baseline_oracle.push(oracle_cur, collection_name, oracle_result[group_param])

                    mongo_result.clear()
                    oracle_result.clear()

    oracle_con.commit()

    close_connection(oracle_con, oracle_cur)

    log.i("Done :::: " + filename + " ::::::::", HUAWEI_VENDOR, frequency_type)
    log.i("----- Parser done ----", HUAWEI_VENDOR, frequency_type)


def remove_word_inside_symbol(text, start_symbol, end_symbol, is_remove_symbol):
    replaced_text = str(text[text.index(start_symbol) + 1:text.rindex(end_symbol)])

    if is_remove_symbol:
        return text.replace(start_symbol + replaced_text + end_symbol, '')
    else:
        return text.replace(replaced_text, '')


def remove_xml_descriptor(text):
    text = str(text)
    text = remove_word_inside_symbol(text, '{', '}', True).strip()
    return text


def param_value_pair_splitter(value_pair_text):
    value_pair_text = str(value_pair_text).strip()
    extracted_value = value_pair_text.split(':')
    return extracted_value[0].strip(), extracted_value[1].strip()


def get_nename(tree):
    xpath = './/spec:syndata'

    class_node_collections = tree.xpath(xpath, namespaces=xml_namespaces)

    for class_node_collection in class_node_collections:
        for class_node in class_node_collection:
            for group_node in class_node:
                group_param = remove_xml_descriptor(group_node.tag).upper()

                if group_param != "NE":
                    continue

                for attribute_node in group_node:
                    for attribute in attribute_node:

                        try:
                            param = remove_xml_descriptor(attribute.tag)
                            value = str(attribute.text).strip()

                            if param == "NENAME":
                                return value

                        except:
                            # traceback.print_exc()
                            continue

    return ""

def get_gnodeB(tree):
    xpath = './/spec:syndata[@FunctionType="gNodeBFunction"]'

    class_node_collections = tree.xpath(xpath, namespaces=xml_namespaces)

    for class_node_collection in class_node_collections:
        for class_node in class_node_collection:
            for group_node in class_node:
                group_param = remove_xml_descriptor(group_node.tag).upper()

                if group_param != "gNodeBFunction".upper():
                    continue

                for attribute_node in group_node:
                    for attribute in attribute_node:

                        try:
                            param = remove_xml_descriptor(attribute.tag)
                            value = str(attribute.text).strip()

                            if str(param).upper() == "gNodeBFunctionName".upper():
                                return value

                        except:
                            # traceback.print_exc()
                            continue

    return ""

def get_gnodeB_xpath(node):    

    t = node.xpath(f'.//class', namespaces=xml_namespaces)
    t1 = node.xpath(f'.//gNodeBFunction', namespaces=xml_namespaces)
    t2 = node.xpath(f'.//gNodeBFunction/attributes', namespaces=xml_namespaces)
    t3 = node.xpath(f'.//gNodeBFunction/attributes/gNodeBFunctionName', namespaces=xml_namespaces)
    class_node_collections = node.xpath(f'.//class/gNodeBFunction/attributes/gNodeBFunctionName/text()', namespaces=xml_namespaces)

    for node in class_node_collections:
        return node

    return ""



def get_swversion_4g(tree):
    xpath = './/spec:syndata'

    class_node_collections = tree.xpath(xpath, namespaces=xml_namespaces)

    for class_node_collection in class_node_collections:
        for class_node in class_node_collection:
            for group_node in class_node:
                group_param = remove_xml_descriptor(group_node.tag).upper()

                if group_param != "NE":
                    continue

                for attribute_node in group_node:
                    for attribute in attribute_node:

                        try:
                            param = remove_xml_descriptor(attribute.tag)
                            value = str(attribute.text).strip()

                            if param == "SWVERSION":
                                return value

                        except:
                            traceback.print_exc()
                            continue

    return ""


def get_productversion(tree):
    xpath = './/spec:syndata'

    class_node_collections = tree.xpath(xpath, namespaces=xml_namespaces)

    for class_node_collection in class_node_collections:
        for class_node in class_node_collection:
            for group_node in class_node:
                group_param = remove_xml_descriptor(group_node.tag).upper()

                if group_param != "NE":
                    continue

                for attribute_node in group_node:
                    for attribute in attribute_node:

                        try:
                            param = remove_xml_descriptor(attribute.tag)
                            value = str(attribute.text).strip()

                            if param == "PRODUCTVERSION":
                                value = value.split(" ")
                                return value[0]

                        except:
                            # traceback.print_exc()
                            continue

    return ""


def get_nefunction(tree, nename):
    xpath = './/spec:syndata'

    class_node_collections = tree.xpath(xpath, namespaces=xml_namespaces)

    function_type = ""

    for class_node_collection in class_node_collections:
        attrib = class_node_collection.attrib

        for item in attrib:

            if item == "FunctionType":
                function_type = attrib[item]

            else:
                if nename in attrib[item]:
                    return function_type.replace('Function', '')

    return ""


def get_4g_root_cell(tree):
    xpath = './/spec:syndata'

    cell = {}

    class_node_collection_root = tree.xpath(xpath, namespaces=xml_namespaces)
    # class_node_collection = tree.xpath(xpath, namespaces=xml_namespaces)
    for class_node_collection in class_node_collection_root:
        for class_node in class_node_collection:
            for group_node in class_node:
                group_param = remove_xml_descriptor(group_node.tag).upper()

                if group_param != "CELL":
                    continue

                localcellid = ""

                for attribute_node in group_node:
                    for attribute in attribute_node:

                        try:
                            key = remove_xml_descriptor(attribute.tag)
                            value = str(attribute.text).strip()

                            if key == "LocalCellId":
                                localcellid = value
                            elif key == "CellName":
                                cell[localcellid] = value
                            else:
                                continue

                            # log.i("Root_cell Key:" + key + ", Value:" + value)

                        except:
                            # traceback.print_exc()
                            continue

    log.i("----------------------------------------------------------", HUAWEI_VENDOR, "4G")
    return cell

# CR2020 - Add 5G NR Cell
def get_5g_root_cell(tree):
    xpath = './/spec:syndata[@FunctionType="gNodeBFunction"]'

    cell = {}

    class_node_collection_root = tree.xpath(xpath, namespaces=xml_namespaces)
    # class_node_collection = tree.xpath(xpath, namespaces=xml_namespaces)
    for class_node_collection in class_node_collection_root:
        for class_node in class_node_collection:
            for group_node in class_node:
                group_param = remove_xml_descriptor(group_node.tag).upper()

                if group_param != "NRDUCell".upper() and group_param != "NRCell".upper():
                    continue

                localcellid = ""

                for attribute_node in group_node:
                    for attribute in attribute_node:

                        try:
                            key = remove_xml_descriptor(attribute.tag)
                            value = str(attribute.text).strip()

                            if str(key).upper() == "NrCellId".upper() or str(key).upper() == "NrDuCellId".upper():
                                localcellid = value
                            elif str(key).upper() == "CellName".upper() or str(key).upper() == "NrDuCellName".upper():
                                cell[localcellid] = value
                            else:
                                continue

                            # log.i("Root_cell Key:" + key + ", Value:" + value)

                        except:
                            # traceback.print_exc()
                            continue

    log.i("----------------------------------------------------------", HUAWEI_VENDOR, "5G")
    return cell

# CR2020 - Add 5G NR Cell
def get_5g_cell_xpath(node):    
    
    cell = {}

    # NRCell
    class_node_collection_root = node.xpath(f'.//NRCell', namespaces=xml_namespaces)    
    for nrcell in class_node_collection_root:
        try:
            # Get cell Id 
            cellIds = nrcell.xpath(f'.//attributes/NrCellId/text()')[0]
            cellName = nrcell.xpath(f'.//attributes/CellName/text()')[0]
            cell[cellIds] = cellName

        except Exception as e:
            log.e(f"Not found NRCell {str(e)}")
    
    # NRDUCell
    class_node_collection_root = node.xpath(f'.//NRDUCell', namespaces=xml_namespaces)    
    for nrcell in class_node_collection_root:
        try:
            # Get cell Id 
            cellIds = nrcell.xpath(f'.//attributes/NrDuCellId/text()')[0]
            cellName = nrcell.xpath(f'.//attributes/NrDuCellName/text()')[0]
            cell[cellIds] = cellName

        except Exception as e:
            log.e(f"Not found NRCell {str(e)}")

    log.i("----------------------------------------------------------", HUAWEI_VENDOR, "5G")
    return cell


# For 2G, 3G
def get_sysobjectid(raw_file):
    with open(raw_file) as f:
        for line in f:

            if not line.startswith("SET SYS:"):
                continue

            extracted_by_colon_line = line.split(':')

            if extracted_by_colon_line.__len__() <= 1:
                continue

            split_line = extracted_by_colon_line[0].split(' ')

            if split_line.__len__() <= 1:
                continue

            value_zone = extracted_by_colon_line[1].split(',')

            for value_pair in value_zone:
                extracted_value_pair = value_pair.split('=')
                key = str(extracted_value_pair[0]).strip()
                value = str(extracted_value_pair[1]).strip()

                if key == KEY_SYSOBJECTID:
                    return value.strip().replace('\"', '')

    return ""


# For 2G, 3G
def get_sysdesc(raw_file):
    with open(raw_file) as f:
        for line in f:

            if not line.startswith("SET SYS:SYSDESC"):
                continue

            extracted_by_colon_line = line.split(':')

            if extracted_by_colon_line.__len__() <= 1:
                continue

            split_line = extracted_by_colon_line[0].split(' ')

            if split_line.__len__() <= 1:
                continue

            value_zone = extracted_by_colon_line[1].split(',')

            for value_pair in value_zone:
                extracted_value_pair = value_pair.split('=')
                key = str(extracted_value_pair[0]).strip()
                value = str(extracted_value_pair[1]).strip()

                if key == "SYSDESC":
                    return value.strip().replace('\"', '')

    return ""


# For 2G, 3G
def get_swversion_2g_3g(raw_file):
    with open(raw_file) as f:
        for line in f:

            if not line.startswith("//For BAM version:"):
                continue

            extracted_by_colon_line = line.split(':')

            if extracted_by_colon_line.__len__() <= 1:
                continue

            return extracted_by_colon_line[1].strip()

    return ""


def get_nodeb_list(raw_file):
    nodeb: Dict[str, str] = {}

    for e in encodings:
        with open(raw_file, encoding=e, errors='ignore') as f:
            try:
                for line in f:

                    if not line.startswith("ADD UNODEB:"):
                        continue

                    extracted_by_colon_line = line.split(':')

                    if extracted_by_colon_line.__len__() <= 1:
                        continue

                    split_line = extracted_by_colon_line[0].split(' ')

                    if split_line.__len__() <= 1:
                        continue

                    value_zone = extracted_by_colon_line[1].split(',')

                    nodebid = ""
                    nodebname = ""

                    for value_pair in value_zone:

                        extracted_value_pair = value_pair.split('=')
                        key = str(extracted_value_pair[0]).strip()
                        value = str(extracted_value_pair[1]).strip().replace("\"", "")

                        if key == "NODEBID":
                            nodebid = value
                        elif key == "NODEBNAME":
                            nodebname = value
                        else:
                            continue

                        if nodebid != "" and nodebname != "":
                            nodeb[nodebid] = nodebname
                            break
            except UnicodeDecodeError:
                print('got unicode error with %s , trying different encoding' % e)

            else:
                print('opening the file with encoding:  %s ' % e)
                break



    return nodeb


def get_2g_root_cell(raw_file):
    cell = {}

    with open(raw_file) as f:
        for line in f:

            if not line.startswith("ADD GCELL:"):
                continue

            extracted_by_colon_line = line.split(':')

            if extracted_by_colon_line.__len__() <= 1:
                continue

            split_line = extracted_by_colon_line[0].split(' ')

            if split_line.__len__() <= 1:
                continue

            value_zone = extracted_by_colon_line[1].split(',')

            cellid = ""

            for value_pair in value_zone:
                extracted_value_pair = value_pair.split('=')
                key = str(extracted_value_pair[0]).strip()
                value = str(extracted_value_pair[1]).strip().replace("\"", "")

                # mongo_value_pair_dic[key] = value

                if key == "CELLID":
                    cellid = value
                elif key == "CELLNAME":
                    cell[cellid] = value
                else:
                    continue

    return cell


def get_2g_gtrx_cell(raw_file):
    cell = {}

    with open(raw_file) as f:
        for line in f:

            if not line.startswith("ADD GTRX:"):
                continue

            extracted_by_colon_line = line.split(':')

            if extracted_by_colon_line.__len__() <= 1:
                continue

            split_line = extracted_by_colon_line[0].split(' ')

            if split_line.__len__() <= 1:
                continue

            value_zone = extracted_by_colon_line[1].split(',')

            cellid = ""

            for value_pair in value_zone:
                extracted_value_pair = value_pair.split('=')
                key = str(extracted_value_pair[0]).strip()
                value = str(extracted_value_pair[1]).strip().replace("\"", "")

                # mongo_value_pair_dic[key] = value

                if key == "TRXID":
                    cellid = value
                elif key == "CELLID":
                    cell[cellid] = value
                else:
                    continue

    return cell


def get_3g_root_cell(raw_file):


    cell = {}
    encodings = ['utf-8', 'windows-1250', 'windows-1252']

    for e in encodings:
        with open(raw_file, encoding=e, errors='ignore') as f:
            try:
                for line in f:

                    if not line.startswith("ADD UCELLSETUP:"):
                        continue

                    extracted_by_colon_line = line.split(':')

                    if extracted_by_colon_line.__len__() <= 1:
                        continue

                    split_line = extracted_by_colon_line[0].split(' ')

                    if split_line.__len__() <= 1:
                        continue

                    value_zone = extracted_by_colon_line[1].split(',')

                    cellid = ""

                    for value_pair in value_zone:
                        extracted_value_pair = value_pair.split('=')
                        key = str(extracted_value_pair[0]).strip()
                        value = str(extracted_value_pair[1]).strip().replace("\"", "")

                        # mongo_value_pair_dic[key] = value

                        if key == KEY_CELLID:
                            cellid = value
                        elif key == KEY_CELLNAME:
                            cell[cellid] = value
                        else:
                            continue
            except UnicodeDecodeError:
                print('got unicode error with %s , trying different encoding' % e)

            else:
                print('opening the file with encoding:  %s ' % e)
                break
    log.i("----------------------------------------------------------", HUAWEI_VENDOR, "3G")
    return cell


def get_sectoreqmref(tree):
    cell = {}

    xpath = './/spec:syndata'

    class_node_collections = tree.xpath(xpath, namespaces=xml_namespaces)

    for class_node_collection in class_node_collections:

        for class_node in class_node_collection:

            for group_node in class_node:

                group_param = remove_xml_descriptor(group_node.tag)

                if group_param != "SECTOREQM":
                    continue

                sectoreqmid = ""

                for attribute_node in group_node:
                    for attribute in attribute_node:

                        try:
                            key = remove_xml_descriptor(attribute.tag)
                            value = str(attribute.text).strip()

                            if key.upper() == 'SECTOREQMID':
                                sectoreqmid = value
                            elif key.upper() == 'SECTORID':

                                cell[sectoreqmid] = value
                            else:
                                continue
                        except:
                            # traceback.print_exc()
                            pass

    return cell

# 5G parsing huawei
def strip_ns_prefix(tree):
	#xpath query for selecting all element nodes in namespace
	query = "descendant-or-self::*[namespace-uri()!='']"
	#for each element returned by the above xpath query...
	for element in tree.xpath(query):
		#replace element name with its local name
		element.tag = etree.QName(element).localname
	return tree
