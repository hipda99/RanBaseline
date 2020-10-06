import traceback

import cx_Oracle

import log
from environment import *
from scr.config.parser import data_source_parser
from scr.helper.mapping_helper import field_mapping_parser
from scr.helper.naming_helper import START_PARSER, FINISH_PARSER
from scr.parser.ericsson import ericsson_baseline_parser
from scr.parser.huewei import huewei_baseline_parser
from scr.parser.zte import zte_baseline_parser
# from scr.dao import ran_baseline_oracle

code_key_for_secure_access = "1c39b674-35cb-4eb5-b66e-7eae69d90604"


def close_connection(connection, cur):
    cur.close()
    connection.close()


def open_connection():
    dsn_tns = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, ORACLE_SID)
    connection = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, dsn_tns)
    cur = connection.cursor()
    return connection, cur


def run_baseline(vendor, frequency_type=""):
    log.i(START_PARSER.format(vendor), vendor)
    log.i("frequency_type : " + frequency_type, vendor)

    oracle_con, oracle_cur = open_connection()

    source_collection = data_source_parser.read_data_source(vendor)

    for source in source_collection:

        if source.FrequencyType == frequency_type:

            try:

                if vendor == ZTE_VENDOR:

                    if source.FrequencyType == '3G':
                        field_mapping_dic, base_mapping_850_dic, base_mapping_2100_dic, red_mapping_dic, cell_level_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency)
                        zte_baseline_parser.prepare_oracle_table_3g(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_850_dic, base_mapping_2100_dic, red_mapping_dic, False)
                    else:
                        field_mapping_dic, base_mapping_dic, red_mapping_dic, cell_level_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency)
                        zte_baseline_parser.prepare_oracle_table(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_dic, red_mapping_dic, False)

                elif vendor == HUAWEI_VENDOR:

                    if source.FrequencyType == "4G":
                        field_mapping_dic, base_mapping_900_dic, base_mapping_1800_dic, base_mapping_2100_dic, param_cell_level_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency)
                        huewei_baseline_parser.prepare_oracle_table_4g(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_900_dic, base_mapping_1800_dic, base_mapping_2100_dic, False)
                    elif source.FrequencyType == "3G":

                        field_mapping_dic, base_mapping_850bma_dic, base_mapping_850upc_dic, base_mapping_2100_dic, param_cell_level_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency)
                        huewei_baseline_parser.prepare_oracle_table_3g(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_850bma_dic, base_mapping_850upc_dic, base_mapping_2100_dic, False)
                    else:
                        field_mapping_dic, base_mapping_dic, param_cell_level_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency)
                        huewei_baseline_parser.prepare_oracle_table_2g(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_dic, False)

                elif vendor == ERICSSON_VENDOR:

                    log.i("FrequencyType : " + source.FrequencyType)

                    if source.FrequencyType == "4G":

                        field_mapping_dic, base_mapping_900_dic, base_mapping_1800_dic, base_mapping_2100_dic, param_cell_level_dic, param_mo_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency)
                        ericsson_baseline_parser.prepare_oracle_table_4g(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_900_dic, base_mapping_1800_dic, base_mapping_2100_dic, False)
                    else:
                        field_mapping_dic, base_mapping_dic, param_cell_level_dic, param_mo_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency)
                        ericsson_baseline_parser.prepare_oracle_table(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_dic, False)


            except Exception:
                # log.e('#################################### Error occur (004): ', vendor)
                # log.i(e, vendor)
                traceback.print_exc()
                # log.e('#################################### Error ', vendor)

    close_connection(oracle_con, oracle_cur)
    log.i(FINISH_PARSER.format(vendor), vendor)


def run(vendor, frequency_type=""):
    log.i(START_PARSER.format(vendor), vendor)
    log.i("frequency_type : " + frequency_type, vendor)

    oracle_con, oracle_cur = open_connection()

    source_collection = data_source_parser.read_data_source(vendor)

    for source in source_collection:

        if source.FrequencyType == frequency_type:

            try:
                # Add initial to prevent error unbound
                field_mapping_dic = {}
                param_cell_level_dic = {}
                param_mo_dic = {}

                if vendor == ZTE_VENDOR:

                    if source.FrequencyType == '3G':
                        field_mapping_dic, base_mapping_850_dic, base_mapping_2100_dic, red_mapping_dic, cell_level_dic, baseline_label_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency)
                        zte_baseline_parser.prepare_oracle_table_3g(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_850_dic, base_mapping_2100_dic, red_mapping_dic, True, baseline_label_dic)
                    else:
                        field_mapping_dic, base_mapping_dic, red_mapping_dic, cell_level_dic, baseline_label_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency)
                        zte_baseline_parser.prepare_oracle_table(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_dic, red_mapping_dic, True, baseline_label_dic)

                    zte_baseline_parser.run(source, field_mapping_dic, cell_level_dic)

                elif vendor == HUAWEI_VENDOR:

                    if source.FrequencyType == "4G":
                        field_mapping_dic, base_mapping_900_dic, base_mapping_1800_dic, base_mapping_2100_dic, base_mapping_2600_dic, param_cell_level_dic, baseline_label_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency)
                        huewei_baseline_parser.prepare_oracle_table_4g(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_900_dic, base_mapping_1800_dic, base_mapping_2100_dic, True, baseline_label_dic)
                    elif source.FrequencyType == "3G":

                        field_mapping_dic, base_mapping_850bma_dic, base_mapping_850upc_dic, base_mapping_2100_dic, param_cell_level_dic, baseline_label_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency)
                        huewei_baseline_parser.prepare_oracle_table_3g(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_850bma_dic, base_mapping_850upc_dic, base_mapping_2100_dic, True, baseline_label_dic)
                    else:
                        field_mapping_dic, base_mapping_dic, param_cell_level_dic, baseline_label_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency)
                        huewei_baseline_parser.prepare_oracle_table_2g(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_dic, True, baseline_label_dic)

                    huewei_baseline_parser.run(source, field_mapping_dic, param_cell_level_dic)
                elif vendor == ERICSSON_VENDOR:

                    log.i("FrequencyType : " + source.FrequencyType)

                    if source.FrequencyType == "4G":

                        field_mapping_dic, base_mapping_900_dic, base_mapping_1800_dic, base_mapping_2100_dic, base_mapping_2600_dic, param_cell_level_dic, param_mo_dic, base_mapping_label_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency)
                        ericsson_baseline_parser.prepare_oracle_table_4g(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_900_dic, base_mapping_1800_dic, base_mapping_2100_dic, base_mapping_2600_dic, True, base_mapping_label_dic)

                        # ran_baseline_oracle.create_trigger(oracle_cur)
                    elif source.FrequencyType == "3G":
                        field_mapping_dic, base_mapping_dic, param_cell_level_dic, param_mo_dic, base_mapping_label_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency)
                        ericsson_baseline_parser.prepare_oracle_table(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_dic, True, base_mapping_label_dic)

                    elif source.FrequencyType == "5G":
                        field_mapping_dic, base_mapping_2600_dic, param_cell_level_dic, param_mo_dic, base_mapping_label_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency)
                        ericsson_baseline_parser.prepare_oracle_table_5g(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_2600_dic, True, base_mapping_label_dic)

                    ericsson_baseline_parser.run(source, field_mapping_dic, param_cell_level_dic, param_mo_dic)
                elif vendor == ERICSSON_SW_VENDOR:

                    ericsson_baseline_parser.prepare_oracle_table_software(oracle_con, oracle_cur, source.FrequencyType)

                    ericsson_baseline_parser.run_sw(source)
                elif vendor == ERICSSON_FEATURE_VENDOR:

                    field_mapping_dic, base_mapping_dic, key_dic = field_mapping_parser.read_mapping(vendor, source.FileMappingPath, source.FrequencyType, source.Frequency, source.FileMappingFeaturePath)
                    ericsson_baseline_parser.prepare_oracle_table_feature(oracle_con, oracle_cur, source.FrequencyType, field_mapping_dic, base_mapping_dic)

                    ericsson_baseline_parser.run_feature(source, field_mapping_dic, key_dic)


            except Exception:
                # log.e('#################################### Error occur (004): ', vendor)
                # log.i(e, vendor)
                traceback.print_exc()
                # log.e('#################################### Error ', vendor)

    close_connection(oracle_con, oracle_cur)
    log.i(FINISH_PARSER.format(vendor), vendor)

# if __name__ == '__main__':
#     if os.environ['SECURE'] == '1c39b674-35cb-4eb5-b66e-7eae69d90604':
#         run(ZTE_VENDOR)
# run(ERICSSON_VENDOR)
# run(HUAWEI_VENDOR)
